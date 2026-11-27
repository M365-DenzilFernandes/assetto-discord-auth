import express from "express";
import axios from "axios";
import crypto from "node:crypto";
const app = express();
app.use(express.json({ limit: "8kb" }));
// =========================================================
// Environment configuration
// =========================================================
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const REDIRECT_URI = process.env.REDIRECT_URI;
const SPONSOR_REQUEST_WEBHOOK_URL =
    process.env.SPONSOR_REQUEST_WEBHOOK_URL;
const UPSTASH_REDIS_REST_URL =
    process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_REDIS_REST_TOKEN =
    process.env.UPSTASH_REDIS_REST_TOKEN;
// Short-lived process cache.
//
// Upstash Redis is the durable source of truth.
// This object only avoids repeated Redis reads while the
// current Render process remains alive.
const authenticatedUsers = {};
// Redis key namespace.
const DISCORD_LINK_KEY_PREFIX =
    "mydragypro:oauth:";
// Presence is intentionally short-lived. A client refreshes its record every
// 45 seconds and Redis removes it automatically after 90 seconds if the game
// closes, crashes, or loses connectivity.
const PRESENCE_KEY_PREFIX =
    "mydragypro:presence:";
const PRESENCE_INDEX_KEY =
    "mydragypro:presence:index";
const PRESENCE_TTL_SECONDS = 90;
const PRESENCE_HEARTBEAT_SECONDS = 45;
const PRESENCE_STALE_SECONDS = 90;
const MAX_VISIBLE_PLAYERS = 100;
// Beat My Time challenge records are short-lived coordination messages. Run
// timing remains entirely client-side; only completed, validated result data is
// submitted here.
const CHALLENGE_KEY_PREFIX =
    "mydragypro:challenge:";
const CHALLENGE_INBOX_PREFIX =
    "mydragypro:challenge-inbox:";
const CHALLENGE_OUTBOX_PREFIX =
    "mydragypro:challenge-outbox:";
const CHALLENGE_PAIR_PREFIX =
    "mydragypro:challenge-pair:";
const CHALLENGE_TTL_SECONDS = 2 * 60 * 60;
const MAX_CHALLENGES_PER_LIST = 100;
// The official client polls every 45 seconds. Keep a small drift allowance while
// preventing modified clients from repeatedly consuming Redis commands. This
// process-local guard deliberately uses no Upstash operations.
const CHALLENGE_POLL_MIN_SECONDS = 40;
const challengePolls = new Map();
// The Players roster follows the same 45-second official-client cadence. This
// process-local guard rejects faster polling before any Redis command is issued.
const PLAYER_POLL_MIN_SECONDS = 40;
const playerPolls = new Map();
function allowChallengePoll(acGuid, challengeType = "all") {
    const playerId = normalizeAcGuid(acGuid);
    const pollKey = `${playerId}:${cleanChallengeType(challengeType) || "all"}`;
    const now = Date.now();
    const previous = Number(challengePolls.get(pollKey)) || 0;
    const minimumMs = CHALLENGE_POLL_MIN_SECONDS * 1000;
    const waitMs = minimumMs - (now - previous);
    if (waitMs > 0) {
        return {
            allowed: false,
            retryAfter: Math.max(1, Math.ceil(waitMs / 1000))
        };
    }
    challengePolls.set(pollKey, now);
    // Bound stale process memory without adding timers or external storage.
    if (challengePolls.size > 10000) {
        const staleBefore = now - (minimumMs * 3);
        for (const [key, timestamp] of challengePolls) {
            if (Number(timestamp) < staleBefore) challengePolls.delete(key);
        }
    }
    return { allowed: true, retryAfter: 0 };
}
function resetChallengePollGuard(acGuid) {
    const playerId = normalizeAcGuid(acGuid);
    challengePolls.delete(`${playerId}:all`);
    challengePolls.delete(`${playerId}:beat_my_time`);
    challengePolls.delete(`${playerId}:touge`);
}
function allowPlayerPoll(acGuid) {
    const playerId = normalizeAcGuid(acGuid);
    const now = Date.now();
    const previous = Number(playerPolls.get(playerId)) || 0;
    const minimumMs = PLAYER_POLL_MIN_SECONDS * 1000;
    const waitMs = minimumMs - (now - previous);
    if (waitMs > 0) {
        return {
            allowed: false,
            retryAfter: Math.max(1, Math.ceil(waitMs / 1000))
        };
    }
    playerPolls.set(playerId, now);
    // Opportunistic cleanup keeps the map bounded without timers or Redis work.
    if (playerPolls.size > 10000) {
        const staleBefore = now - (minimumMs * 3);
        for (const [key, timestamp] of playerPolls) {
            if (Number(timestamp) < staleBefore) playerPolls.delete(key);
        }
    }
    return { allowed: true, retryAfter: 0 };
}
// =========================================================
// General helpers
// =========================================================
function cleanText(value, maxLength = 200) {
    if (value === undefined || value === null) {
        return "";
    }
    return String(value)
        .replace(/@everyone/gi, "@ everyone")
        .replace(/@here/gi, "@ here")
        .replace(/[<>]/g, "")
        .trim()
        .slice(0, maxLength);
}
function normalizeAcGuid(value) {
    return cleanText(value, 100);
}
function redisKeyForAcGuid(acGuid) {
    return `${DISCORD_LINK_KEY_PREFIX}${acGuid}`;
}
function assertUpstashConfigured() {
    if (
        !UPSTASH_REDIS_REST_URL ||
        !UPSTASH_REDIS_REST_TOKEN
    ) {
        throw new Error(
            "Upstash Redis environment variables are not configured."
        );
    }
}
function upstashHeaders() {
    return {
        Authorization:
            `Bearer ${UPSTASH_REDIS_REST_TOKEN}`,
        "Content-Type": "text/plain"
    };
}
function upstashJsonHeaders() {
    return {
        Authorization:
            `Bearer ${UPSTASH_REDIS_REST_TOKEN}`,
        "Content-Type": "application/json"
    };
}
function upstashBaseUrl() {
    return String(UPSTASH_REDIS_REST_URL || "")
        .replace(/\/+$/, "");
}
// Upstash pipelines keep a heartbeat to one HTTPS round trip even though it
// updates both the expiring record and the sorted-set index.
async function upstashPipeline(commands) {
    assertUpstashConfigured();
    if (!Array.isArray(commands) || commands.length === 0) {
        return [];
    }
    const response = await axios.post(
        `${upstashBaseUrl()}/pipeline`,
        commands,
        {
            headers: upstashJsonHeaders(),
            timeout: 10000
        }
    );
    if (!Array.isArray(response.data)) {
        throw new Error(
            "Upstash pipeline returned an invalid response."
        );
    }
    return response.data.map((item, index) => {
        if (item?.error) {
            throw new Error(
                `Upstash pipeline command ${index + 1} failed: ${item.error}`
            );
        }
        return item?.result;
    });
}
async function upstashCommand(command, ...args) {
    const results = await upstashPipeline([
        [command, ...args]
    ]);
    return results[0];
}
function presenceKeyForAcGuid(acGuid) {
    return `${PRESENCE_KEY_PREFIX}${normalizeAcGuid(acGuid)}`;
}
function nowUnixSeconds() {
    return Math.floor(Date.now() / 1000);
}
function cleanHash(value) {
    const hash = cleanText(value, 64).toLowerCase();
    return /^[a-f0-9]{64}$/.test(hash) ? hash : "";
}
function cleanAvailability(value) {
    const availability = cleanText(value, 20).toLowerCase();
    return ["available", "busy", "dnd"].includes(availability)
        ? availability
        : "available";
}
function cleanChallengeAccess(value) {
    const access = cleanText(value, 20).toLowerCase();
    return ["friends", "everyone", "off"].includes(access)
        ? access
        : "friends";
}
function cleanModeKey(value) {
    const modeKey = cleanText(value, 100).toLowerCase();
    return /^[a-z0-9:_-]{1,100}$/.test(modeKey)
        ? modeKey
        : "";
}
function cleanChallengeType(value) {
    const type = cleanText(value, 40).toLowerCase();
    return type === "touge" ? "touge" : type === "beat_my_time" ? "beat_my_time" : "";
}
const TOUGE_RULES = Object.freeze({
    beginner: Object.freeze({ gap_limit_m: 300, grace_seconds: 4 }),
    intermediate: Object.freeze({ gap_limit_m: 200, grace_seconds: 3 }),
    pro: Object.freeze({ gap_limit_m: 100, grace_seconds: 2 })
});
function cleanTougeDifficulty(value) {
    const key = cleanText(value, 20).toLowerCase();
    return Object.prototype.hasOwnProperty.call(TOUGE_RULES, key) ? key : "intermediate";
}
function cleanTougeRole(value) {
    const role = cleanText(value, 20).toLowerCase();
    return role === "follower" ? "follower" : "leader";
}
function parseStoredJson(value, label = "record") {
    if (value === null || value === undefined || value === "") {
        return null;
    }
    try {
        return typeof value === "string"
            ? JSON.parse(value)
            : value;
    } catch {
        throw new Error(`Stored ${label} is invalid JSON.`);
    }
}
function challengeKey(challengeId) {
    return `${CHALLENGE_KEY_PREFIX}${cleanText(challengeId, 64)}`;
}
function challengeInboxKey(acGuid) {
    return `${CHALLENGE_INBOX_PREFIX}${normalizeAcGuid(acGuid)}`;
}
function challengeOutboxKey(acGuid) {
    return `${CHALLENGE_OUTBOX_PREFIX}${normalizeAcGuid(acGuid)}`;
}
function challengePairKey(firstPlayer, secondPlayer) {
    const pair = [
        normalizeAcGuid(firstPlayer),
        normalizeAcGuid(secondPlayer)
    ].sort().join("\u0000");
    const digest = crypto
        .createHash("sha256")
        .update(pair)
        .digest("hex");
    return `${CHALLENGE_PAIR_PREFIX}${digest}`;
}
function challengeTtlRemaining(challenge, now = nowUnixSeconds()) {
    return Math.max(1, Number(challenge?.expires_at || 0) - now);
}
async function getPresenceRecord(acGuid) {
    const stored = await upstashCommand(
        "GET",
        presenceKeyForAcGuid(acGuid)
    );
    return parseStoredJson(stored, "presence record");
}
async function getChallengeRecord(challengeId) {
    const id = cleanText(challengeId, 64);
    if (!/^[a-f0-9]{32}$/.test(id)) return null;
    const stored = await upstashCommand("GET", challengeKey(id));
    return parseStoredJson(stored, "challenge record");
}
function challengeIsActive(challenge, now = nowUnixSeconds()) {
    return challenge
        && Number(challenge.expires_at || 0) > now
        && ["pending", "accepted"].includes(challenge.status);
}
function publicChallenge(challenge) {
    if (!challenge || typeof challenge !== "object") return null;
    const type = cleanChallengeType(challenge.type) || "beat_my_time";
    return {
        id: cleanText(challenge.id, 64),
        type,
        from_player_id: cleanText(challenge.from_player_id, 100),
        from_driver_name: cleanText(challenge.from_driver_name, 100),
        to_player_id: cleanText(challenge.to_player_id, 100),
        to_driver_name: cleanText(challenge.to_driver_name, 100),
        from_car_id: cleanText(challenge.from_car_id, 100),
        to_car_id: cleanText(challenge.to_car_id, 100),
        server_hash: cleanHash(challenge.server_hash),
        track_hash: cleanHash(challenge.track_hash),
        challenger_role: type === "touge" ? cleanTougeRole(challenge.challenger_role) : "",
        difficulty: type === "touge" ? cleanTougeDifficulty(challenge.difficulty) : "",
        gap_limit_m: Number(challenge.gap_limit_m) || 0,
        grace_seconds: Number(challenge.grace_seconds) || 0,
        duration_seconds: Number(challenge.duration_seconds) || 0,
        mode_key: cleanModeKey(challenge.mode_key),
        mode_label: cleanText(challenge.mode_label, 120),
        target_ms: Number(challenge.target_ms) || 0,
        source_run_ref: cleanText(challenge.source_run_ref, 100),
        source_run_timestamp: Number(challenge.source_run_timestamp) || 0,
        status: cleanText(challenge.status, 20),
        created_at: Number(challenge.created_at) || 0,
        accepted_at: Number(challenge.accepted_at) || 0,
        expires_at: Number(challenge.expires_at) || 0,
        attempts: Number(challenge.attempts) || 0,
        best_ms: Number(challenge.best_ms) || 0,
        last_attempt_ms: Number(challenge.last_attempt_ms) || 0,
        last_attempt_at: Number(challenge.last_attempt_at) || 0,
        beaten: challenge.beaten === true,
        completed_at: Number(challenge.completed_at) || 0,
        terminal_at: Number(challenge.terminal_at) || 0,
        winner_player_id: cleanText(challenge.winner_player_id, 100),
        winner_role: cleanText(challenge.winner_role, 20),
        result_reason: cleanText(challenge.result_reason, 80),
        result_elapsed_ms: Number(challenge.result_elapsed_ms) || 0,
        final_gap_m: Number(challenge.final_gap_m) || 0,
        min_gap_m: Number(challenge.min_gap_m) || 0,
        max_gap_m: Number(challenge.max_gap_m) || 0,
        version: Number(challenge.version) || (type === "touge" ? 2 : 1)
    };
}
function noStore(res) {
    res.set("Cache-Control", "no-store");
}
async function requireLinkedIdentity(req, res, next) {
    const acGuid = normalizeAcGuid(
        req.body?.ac_guid || req.query?.ac_guid
    );
    if (!acGuid) {
        return res.status(400).json({
            success: false,
            error: "Missing Assetto Corsa identity."
        });
    }
    try {
        const link = await getDiscordLink(acGuid);
        if (!link?.discord_id) {
            return res.status(401).json({
                success: false,
                error: "Discord authentication is required."
            });
        }
        req.mdpIdentity = {
            acGuid,
            discordId: link.discord_id
        };
        return next();
    } catch (error) {
        console.error(
            "[PRESENCE] Identity verification unavailable:",
            error.response?.status || error.message
        );
        return res.status(503).json({
            success: false,
            verification_unavailable: true,
            error: "Identity verification is temporarily unavailable."
        });
    }
}
// =========================================================
// Durable Discord-link storage
// =========================================================
async function saveDiscordLink(
    acGuid,
    discordId
) {
    assertUpstashConfigured();
    const normalizedGuid =
        normalizeAcGuid(acGuid);
    const normalizedDiscordId =
        cleanText(discordId, 30);
    if (!normalizedGuid) {
        throw new Error(
            "Cannot save Discord link without an AC GUID."
        );
    }
    if (!/^\d{5,30}$/.test(normalizedDiscordId)) {
        throw new Error(
            "Cannot save Discord link with an invalid Discord ID."
        );
    }
    const redisKey =
        redisKeyForAcGuid(normalizedGuid);
    const record = {
        discord_id: normalizedDiscordId,
        linked_at: new Date().toISOString(),
        version: 1
    };
    // Upstash REST treats the POST body as the final
    // argument to the Redis SET command.
    const response = await axios.post(
        `${UPSTASH_REDIS_REST_URL}/set/` +
        encodeURIComponent(redisKey),
        JSON.stringify(record),
        {
            headers: upstashHeaders(),
            timeout: 10000
        }
    );
    if (response.data?.result !== "OK") {
        throw new Error(
            "Upstash did not confirm the Discord link write."
        );
    }
    authenticatedUsers[normalizedGuid] =
        normalizedDiscordId;
    return record;
}
async function getDiscordLink(acGuid) {
    assertUpstashConfigured();
    const normalizedGuid =
        normalizeAcGuid(acGuid);
    if (!normalizedGuid) {
        return null;
    }
    // Use the local process cache first.
    if (authenticatedUsers[normalizedGuid]) {
        return {
            discord_id:
                authenticatedUsers[normalizedGuid],
            source: "memory"
        };
    }
    const redisKey =
        redisKeyForAcGuid(normalizedGuid);
    const response = await axios.get(
        `${UPSTASH_REDIS_REST_URL}/get/` +
        encodeURIComponent(redisKey),
        {
            headers: {
                Authorization:
                    `Bearer ${UPSTASH_REDIS_REST_TOKEN}`
            },
            timeout: 10000
        }
    );
    const storedValue = response.data?.result;
    // A successful Redis GET with a null result means
    // the user does not have a stored link.
    if (
        storedValue === null ||
        storedValue === undefined ||
        storedValue === ""
    ) {
        return null;
    }
    let record;
    try {
        record =
            typeof storedValue === "string"
                ? JSON.parse(storedValue)
                : storedValue;
    } catch {
        throw new Error(
            "Stored Discord-link record is invalid JSON."
        );
    }
    const discordId = cleanText(
        record?.discord_id,
        30
    );
    if (!/^\d{5,30}$/.test(discordId)) {
        throw new Error(
            "Stored Discord-link record has an invalid Discord ID."
        );
    }
    // Repopulate the process cache after Render wakes.
    authenticatedUsers[normalizedGuid] =
        discordId;
    return {
        discord_id: discordId,
        linked_at: record.linked_at || null,
        source: "upstash"
    };
}
// =========================================================
// Existing Discord authentication
// =========================================================
app.get("/login", (req, res) => {
    const acGuid =
        normalizeAcGuid(req.query.ac_guid);
    if (!acGuid) {
        return res.status(400).send(
            "Missing or invalid Assetto Corsa identity."
        );
    }
    const url =
        `https://discord.com/oauth2/authorize` +
        `?client_id=${encodeURIComponent(CLIENT_ID)}` +
        `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
        `&response_type=code` +
        `&scope=identify` +
        `&state=${encodeURIComponent(acGuid)}`;
    return res.redirect(url);
});
function renderAuthPage({ success, title, message }) {
    const accent = success ? "#55d68b" : "#ff5c70";
    const icon = success ? "âœ“" : "!";
    const status = success ? "Discord Connected" : "Connection Failed";
    return `<!DOCTYPE html>
<html lang="en">
<head>
 <meta charset="UTF-8">
 <meta name="viewport" content="width=device-width, initial-scale=1">
 <meta name="theme-color" content="#090b12">
 <title>${title}</title>
 <style>
 * { box-sizing: border-box; }
 html, body { margin: 0; min-height: 100%; }
 body {
 min-height: 100vh;
 display: flex;
 align-items: center;
 justify-content: center;
 padding: 24px;
 color: #f4f6ff;
 background:
 radial-gradient(circle at 20% 10%, rgba(88,101,242,.22), transparent 35%),
 radial-gradient(circle at 80% 90%, rgba(39,201,123,.13), transparent 35%),
 linear-gradient(145deg, #080a10, #111521);
 font-family: Inter, "Segoe UI", Arial, sans-serif;
 }
 .card {
 width: min(440px, 100%);
 padding: 38px 32px 30px;
 text-align: center;
 background: rgba(18,22,34,.92);
 border: 1px solid rgba(255,255,255,.09);
 border-radius: 24px;
 box-shadow: 0 24px 70px rgba(0,0,0,.55);
 backdrop-filter: blur(16px);
 }
 .brand {
 display: flex;
 align-items: center;
 justify-content: center;
 gap: 11px;
 margin-bottom: 28px;
 }
 .brand-mark {
 width: 42px;
 height: 42px;
 display: grid;
 place-items: center;
 color: #fff;
 font-size: 15px;
 font-weight: 900;
 letter-spacing: -.5px;
 border-radius: 12px;
 background: linear-gradient(135deg, #5865f2, #7447ff);
 box-shadow: 0 8px 28px rgba(88,101,242,.38);
 }
 .brand-name {
 font-size: 21px;
 font-weight: 800;
 letter-spacing: -.4px;
 }
 .brand-name span { color: #8a93ff; }
 .status-icon {
 width: 72px;
 height: 72px;
 display: grid;
 place-items: center;
 margin: 0 auto 22px;
 color: ${accent};
 font-size: 37px;
 font-weight: 800;
 border: 2px solid ${accent};
 border-radius: 50%;
 background: ${success ? "rgba(85,214,139,.10)" : "rgba(255,92,112,.10)"};
 box-shadow: 0 0 35px ${success ? "rgba(85,214,139,.16)" : "rgba(255,92,112,.16)"};
 }
 .eyebrow {
 margin-bottom: 8px;
 color: ${accent};
 font-size: 12px;
 font-weight: 800;
 letter-spacing: 1.6px;
 text-transform: uppercase;
 }
 h1 {
 margin: 0 0 12px;
 font-size: 28px;
 line-height: 1.15;
 }
 .message {
 margin: 0 auto;
 max-width: 340px;
 color: #aeb5c8;
 font-size: 15px;
 line-height: 1.65;
 }
 button {
 width: 100%;
 margin-top: 28px;
 padding: 14px 20px;
 color: #fff;
 font: inherit;
 font-weight: 750;
 cursor: pointer;
 border: 0;
 border-radius: 12px;
 background: linear-gradient(135deg, #5865f2, #7549f5);
 box-shadow: 0 10px 28px rgba(88,101,242,.28);
 transition: transform .15s ease, filter .15s ease;
 }
 button:hover { transform: translateY(-1px); filter: brightness(1.08); }
 button:active { transform: translateY(0); }
 .close-note {
 min-height: 18px;
 margin-top: 12px;
 color: #777f93;
 font-size: 12px;
 }
 .footer {
 margin-top: 24px;
 padding-top: 20px;
 color: #666e82;
 font-size: 11px;
 border-top: 1px solid rgba(255,255,255,.07);
 }
 </style>
</head>
<body>
 <main class="card">
 <div class="brand">
 <div class="brand-mark">MDP</div>
 <div class="brand-name">MyDragy<span>Pro</span></div>
 </div>
 <div class="status-icon">${icon}</div>
 <div class="eyebrow">${status}</div>
 <h1>${title}</h1>
 <p class="message">${message}</p>
 <button type="button" onclick="closeAuthWindow()">Close Window</button>
 <div class="close-note" id="closeNote">You can safely return to MyDragyPro.</div>
 <div class="footer">Secure authentication powered by Discord</div>
 </main>
 <script>
 function closeAuthWindow() {
 window.close();
 setTimeout(function () {
 document.getElementById("closeNote").textContent =
 "Your browser prevented automatic closing. You can close this tab manually.";
 }, 300);
 }
 </script>
</body>
</html>`;
}
app.get("/callback", async (req, res) => {
    try {
        const code = cleanText(
            req.query.code,
            500
        );
        const acGuid =
            normalizeAcGuid(req.query.state);
        if (!code || !acGuid) {
            return res.status(400).send(
                "Missing OAuth code or Assetto Corsa identity."
            );
        }
        const tokenRes = await axios.post(
            "https://discord.com/api/oauth2/token",
            new URLSearchParams({
                client_id: CLIENT_ID,
                client_secret: CLIENT_SECRET,
                grant_type: "authorization_code",
                code,
                redirect_uri: REDIRECT_URI
            }),
            {
                headers: {
                    "Content-Type":
                        "application/x-www-form-urlencoded"
                },
                timeout: 10000
            }
        );
        const userRes = await axios.get(
            "https://discord.com/api/users/@me",
            {
                headers: {
                    Authorization:
                        `Bearer ${tokenRes.data.access_token}`
                },
                timeout: 10000
            }
        );
        const discordId =
            cleanText(userRes.data?.id, 30);
        // Save durably before displaying OAuth success.
        await saveDiscordLink(
            acGuid,
            discordId
        );
        console.log(
            `[AUTH] OAuth link stored durably: ` +
            `ac_guid=${JSON.stringify(acGuid)} ` +
            `discord_id=${discordId}`
        );
        return res
            .status(200)
            .type("html")
            .send(renderAuthPage({
                success: true,
                title: "Authentication Complete",
                message: "Your Discord account has been securely connected to MyDragyPro. You can now close this window and return to the app."
            }));
    } catch (error) {
        const status =
            error.response?.status;
        const details =
            error.response?.data;
        console.error(
            "[AUTH] OAuth callback failed:",
            status || error.message,
            details || ""
        );
        return res
            .status(502)
            .type("html")
            .send(renderAuthPage({
                success: false,
                title: "Authentication Failed",
                message: "We could not connect your Discord account. Close this window, return to MyDragyPro, and try the authentication process again."
            }));
    }
});
app.get("/check", async (req, res) => {
    const acGuid =
        normalizeAcGuid(req.query.ac_guid);
    if (!acGuid) {
        return res.status(400).json({
            authenticated: false,
            discord_id: null,
            error: "Missing Assetto Corsa identity."
        });
    }
    try {
        const link =
            await getDiscordLink(acGuid);
        console.log(
            `[AUTH] Discord link check: ` +
            `ac_guid=${JSON.stringify(acGuid)} ` +
            `authenticated=${Boolean(link)} ` +
            `source=${link?.source || "none"}`
        );
        return res.json({
            authenticated: Boolean(link?.discord_id),
            discord_id:
                link?.discord_id || null
        });
    } catch (error) {
        console.error(
            "[AUTH] Discord link verification unavailable:",
            error.response?.status || error.message,
            error.response?.data || ""
        );
        // Do not incorrectly report the user as unlinked
        // when the durable store itself is unavailable.
        return res.status(503).json({
            authenticated: false,
            discord_id: null,
            verification_unavailable: true,
            error:
                "Discord-link verification is temporarily unavailable."
        });
    }
});
// =========================================================
// MyDragyPro online presence
// =========================================================
app.post(
    "/presence",
    requireLinkedIdentity,
    async (req, res) => {
        noStore(res);
        try {
            const {
                driver_name,
                car_id,
                car_name,
                server_hash,
                track_hash,
                mode,
                availability,
                challenge_access,
                app_version
            } = req.body || {};
            const acGuid = req.mdpIdentity.acGuid;
            const discordId = req.mdpIdentity.discordId;
            const updatedAt = nowUnixSeconds();
            const record = {
                player_id: acGuid,
                discord_id: discordId,
                driver_name:
                    cleanText(driver_name, 100) || "Driver",
                car_id: cleanText(car_id, 100),
                car_name: cleanText(car_name, 150),
                server_hash: cleanHash(server_hash),
                track_hash: cleanHash(track_hash),
                mode: cleanText(mode, 40),
                availability:
                    cleanAvailability(availability),
                challenge_access:
                    cleanChallengeAccess(challenge_access),
                app_version: cleanText(app_version, 30),
                updated_at: updatedAt,
                version: 1
            };
            await upstashPipeline([
                [
                    "SET",
                    presenceKeyForAcGuid(acGuid),
                    JSON.stringify(record),
                    "EX",
                    PRESENCE_TTL_SECONDS
                ],
                [
                    "ZADD",
                    PRESENCE_INDEX_KEY,
                    updatedAt,
                    acGuid
                ]
            ]);
            return res.status(200).json({
                success: true,
                expires_in: PRESENCE_TTL_SECONDS,
                next_heartbeat_in:
                    PRESENCE_HEARTBEAT_SECONDS,
                server_time: updatedAt
            });
        } catch (error) {
            console.error(
                "[PRESENCE] Heartbeat failed:",
                error.response?.status || error.message,
                error.response?.data || ""
            );
            return res.status(503).json({
                success: false,
                error:
                    "Online presence is temporarily unavailable."
            });
        }
    }
);
app.get(
    "/players",
    requireLinkedIdentity,
    async (req, res) => {
        noStore(res);
        const requesterGuid =
            req.mdpIdentity.acGuid;
        const poll = allowPlayerPoll(requesterGuid);
        if (!poll.allowed) {
            res.set("Retry-After", String(poll.retryAfter));
            return res.status(429).json({
                success: false,
                retry_after_seconds: poll.retryAfter,
                error: "Player list refresh is limited to once every 40 seconds."
            });
        }
        try {
            const requesterPresence =
                await getPresenceRecord(requesterGuid);
            const requesterServerHash =
                cleanHash(requesterPresence?.server_hash);
            const now = nowUnixSeconds();
            const cutoff =
                now - PRESENCE_STALE_SECONDS;
            // Cleanup and lookup are deliberately performed only when a user
            // asks for the roster. Heartbeats never poll the player list.
            const [, indexedIds] = await upstashPipeline([
                [
                    "ZREMRANGEBYSCORE",
                    PRESENCE_INDEX_KEY,
                    0,
                    cutoff
                ],
                [
                    "ZREVRANGEBYSCORE",
                    PRESENCE_INDEX_KEY,
                    now,
                    cutoff,
                    "LIMIT",
                    0,
                    MAX_VISIBLE_PLAYERS
                ]
            ]);
            const playerIds = Array.isArray(indexedIds)
                ? indexedIds
                    .map((value) => normalizeAcGuid(value))
                    .filter((value) =>
                        value && value !== requesterGuid
                    )
                : [];
            let storedRecords = [];
            if (playerIds.length > 0) {
                storedRecords = await upstashCommand(
                    "MGET",
                    ...playerIds.map((playerId) =>
                        presenceKeyForAcGuid(playerId)
                    )
                );
                if (!Array.isArray(storedRecords)) storedRecords = [];
            }
            const players = [];
            for (let index = 0;
                index < playerIds.length;
                index += 1) {
                const storedValue = storedRecords[index];
                if (!storedValue) {
                    continue;
                }
                try {
                    const record =
                        typeof storedValue === "string"
                            ? JSON.parse(storedValue)
                            : storedValue;
                    const updatedAt =
                        Number(record?.updated_at) || 0;
                    // The Discord ID remains server-side. The roster exposes
                    // only the fields needed by the phase-one UI.
                    players.push({
                        player_id:
                            cleanText(record?.player_id, 100),
                        driver_name:
                            cleanText(record?.driver_name, 100),
                        car_id:
                            cleanText(record?.car_id, 100),
                        car_name:
                            cleanText(record?.car_name, 150),
                        server_hash:
                            cleanHash(record?.server_hash),
                        track_hash:
                            cleanHash(record?.track_hash),
                        mode:
                            cleanText(record?.mode, 40),
                        availability:
                            cleanAvailability(
                                record?.availability
                            ),
                        challenge_access:
                            cleanChallengeAccess(
                                record?.challenge_access
                            ),
                        can_challenge:
                            cleanChallengeAccess(record?.challenge_access) !== "off"
                            && cleanAvailability(record?.availability) !== "dnd"
                            && (
                                cleanChallengeAccess(record?.challenge_access) === "everyone"
                                || (
                                    cleanHash(record?.server_hash) !== ""
                                    && cleanHash(record?.server_hash) === requesterServerHash
                                )
                            ),
                        app_version:
                            cleanText(record?.app_version, 30),
                        updated_at: updatedAt,
                        age_seconds:
                            Math.max(0, now - updatedAt)
                    });
                } catch {
                    console.warn(
                        "[PRESENCE] Invalid record skipped:",
                        playerIds[index]
                    );
                }
            }
            return res.status(200).json({
                success: true,
                server_time: now,
                ttl_seconds: PRESENCE_TTL_SECONDS,
                count: players.length,
                players
            });
        } catch (error) {
            console.error(
                "[PRESENCE] Player-list lookup failed:",
                error.response?.status || error.message,
                error.response?.data || ""
            );
            return res.status(503).json({
                success: false,
                players: [],
                error:
                    "Online players are temporarily unavailable."
            });
        }
    }
);
app.post(
    "/presence/offline",
    requireLinkedIdentity,
    async (req, res) => {
        noStore(res);
        try {
            const acGuid = req.mdpIdentity.acGuid;
            await upstashPipeline([
                ["DEL", presenceKeyForAcGuid(acGuid)],
                ["ZREM", PRESENCE_INDEX_KEY, acGuid]
            ]);
            return res.status(200).json({
                success: true,
                server_time: nowUnixSeconds()
            });
        } catch (error) {
            console.error(
                "[PRESENCE] Offline update failed:",
                error.response?.status || error.message,
                error.response?.data || ""
            );
            return res.status(503).json({
                success: false,
                error:
                    "Presence removal is temporarily unavailable."
            });
        }
    }
);
// =========================================================
// Beat My Time challenges (phase two)
// =========================================================
app.post(
    "/challenges",
    requireLinkedIdentity,
    async (req, res) => {
        noStore(res);
        try {
            const fromPlayerId = req.mdpIdentity.acGuid;
            const toPlayerId = normalizeAcGuid(
                req.body?.target_player_id
            );
            const type = cleanChallengeType(req.body?.type);
            let modeKey = cleanModeKey(req.body?.mode_key);
            let modeLabel = cleanText(req.body?.mode_label, 120);
            let targetMs = Math.round(Number(req.body?.target_ms));
            const sourceRunRef = cleanText(req.body?.source_run_ref, 100);
            const sourceRunTimestamp = Math.floor(
                Number(req.body?.source_run_timestamp) || 0
            );
            if (!toPlayerId || toPlayerId === fromPlayerId) {
                return res.status(400).json({
                    success: false,
                    error: "Select another online player."
                });
            }
            if (type !== "beat_my_time" && type !== "touge") {
                return res.status(400).json({
                    success: false,
                    error: "Unsupported challenge type."
                });
            }
            if (type === "touge") {
                modeKey = "touge:basic";
                modeLabel = "Basic Touge";
                targetMs = 0;
            }
            if (!modeKey || !modeLabel) {
                return res.status(400).json({
                    success: false,
                    error: "A valid challenge mode is required."
                });
            }
            if (type === "beat_my_time"
                && (!Number.isFinite(targetMs) || targetMs < 100 || targetMs > 3600000)) {
                return res.status(400).json({
                    success: false,
                    error: "Target time is outside the supported range."
                });
            }
            const now = nowUnixSeconds();
            const [fromPresence, toPresence] = await Promise.all([
                getPresenceRecord(fromPlayerId),
                getPresenceRecord(toPlayerId)
            ]);
            if (!toPresence
                || now - Number(toPresence.updated_at || 0) > PRESENCE_STALE_SECONDS) {
                return res.status(404).json({
                    success: false,
                    error: "That player is no longer online."
                });
            }
            const targetAccess = cleanChallengeAccess(
                toPresence.challenge_access
            );
            const targetAvailability = cleanAvailability(
                toPresence.availability
            );
            const sameServer = cleanHash(fromPresence?.server_hash) !== ""
                && cleanHash(fromPresence?.server_hash) === cleanHash(toPresence.server_hash);
            if (type === "touge" && !sameServer) {
                return res.status(409).json({
                    success: false,
                    error: "Basic Touge requires both players on the same server."
                });
            }
            if (targetAvailability === "dnd" || targetAccess === "off") {
                return res.status(403).json({
                    success: false,
                    error: "That player is not accepting challenges."
                });
            }
            // Phase-two "Friends" is intentionally conservative until a real
            // friend graph exists: only players in the same AC server qualify.
            if (targetAccess === "friends" && !sameServer) {
                return res.status(403).json({
                    success: false,
                    error: "Friends access currently allows same-server players only."
                });
            }
            const pairKey = challengePairKey(fromPlayerId, toPlayerId);
            const existingId = cleanText(
                await upstashCommand("GET", pairKey),
                64
            );
            if (existingId) {
                const existing = await getChallengeRecord(existingId);
                if (challengeIsActive(existing, now)) {
                    return res.status(409).json({
                        success: false,
                        error: "An active challenge already exists between these players.",
                        challenge: publicChallenge(existing)
                    });
                }
                await upstashCommand("DEL", pairKey);
            }
            const challengeId = crypto.randomBytes(16).toString("hex");
            const expiresAt = now + CHALLENGE_TTL_SECONDS;
            const tougeDifficulty = cleanTougeDifficulty(req.body?.difficulty);
            const tougeRules = TOUGE_RULES[tougeDifficulty];
            const challengerRole = cleanTougeRole(req.body?.challenger_role);
            const challenge = {
                id: challengeId,
                type,
                from_player_id: fromPlayerId,
                from_driver_name: cleanText(fromPresence?.driver_name, 100) || "Driver",
                to_player_id: toPlayerId,
                to_driver_name: cleanText(toPresence.driver_name, 100) || "Driver",
                from_car_id: cleanText(fromPresence?.car_id, 100),
                to_car_id: cleanText(toPresence.car_id, 100),
                server_hash: type === "touge" ? cleanHash(toPresence.server_hash) : "",
                track_hash: type === "touge" ? cleanHash(toPresence.track_hash) : "",
                challenger_role: type === "touge" ? challengerRole : "",
                difficulty: type === "touge" ? tougeDifficulty : "",
                gap_limit_m: type === "touge" ? tougeRules.gap_limit_m : 0,
                grace_seconds: type === "touge" ? tougeRules.grace_seconds : 0,
                duration_seconds: type === "touge" ? 180 : 0,
                mode_key: modeKey,
                mode_label: modeLabel,
                target_ms: targetMs,
                source_run_ref: sourceRunRef,
                source_run_timestamp: sourceRunTimestamp,
                status: "pending",
                created_at: now,
                accepted_at: 0,
                expires_at: expiresAt,
                attempts: 0,
                best_ms: 0,
                last_attempt_ms: 0,
                last_attempt_at: 0,
                beaten: false,
                completed_at: 0,
                terminal_at: 0,
                result_ids: [],
                winner_player_id: "",
                winner_role: "",
                result_reason: "",
                result_elapsed_ms: 0,
                final_gap_m: 0,
                min_gap_m: 0,
                max_gap_m: 0,
                version: type === "touge" ? 2 : 1
            };
            await upstashPipeline([
                [
                    "SET",
                    challengeKey(challengeId),
                    JSON.stringify(challenge),
                    "EX",
                    CHALLENGE_TTL_SECONDS
                ],
                ["ZADD", challengeInboxKey(toPlayerId), expiresAt, challengeId],
                ["ZADD", challengeOutboxKey(fromPlayerId), expiresAt, challengeId],
                ["EXPIRE", challengeInboxKey(toPlayerId), CHALLENGE_TTL_SECONDS],
                ["EXPIRE", challengeOutboxKey(fromPlayerId), CHALLENGE_TTL_SECONDS],
                ["SET", pairKey, challengeId, "EX", CHALLENGE_TTL_SECONDS]
            ]);
            resetChallengePollGuard(fromPlayerId);
            return res.status(201).json({
                success: true,
                server_time: now,
                challenge: publicChallenge(challenge)
            });
        } catch (error) {
            console.error(
                "[CHALLENGE] Create failed:",
                error.response?.status || error.message,
                error.response?.data || ""
            );
            return res.status(503).json({
                success: false,
                error: "The challenge could not be created."
            });
        }
    }
);
app.get(
    "/challenges",
    requireLinkedIdentity,
    async (req, res) => {
        noStore(res);
        const playerId = req.mdpIdentity.acGuid;
        // Missing type preserves the phase-two contract for older Beat My Time clients.
        const requestedType = cleanChallengeType(req.query?.type) || "beat_my_time";
        const poll = allowChallengePoll(playerId, requestedType);
        if (!poll.allowed) {
            res.set("Retry-After", String(poll.retryAfter));
            return res.status(429).json({
                success: false,
                rate_limited: true,
                retry_after: poll.retryAfter,
                error: "Challenge updates were requested too quickly."
            });
        }
        try {
            const now = nowUnixSeconds();
            const inboxKey = challengeInboxKey(playerId);
            const outboxKey = challengeOutboxKey(playerId);
            const [, , incomingIds, outgoingIds] = await upstashPipeline([
                ["ZREMRANGEBYSCORE", inboxKey, 0, now - 1],
                ["ZREMRANGEBYSCORE", outboxKey, 0, now - 1],
                [
                    "ZRANGEBYSCORE", inboxKey, now, "+inf",
                    "LIMIT", 0, MAX_CHALLENGES_PER_LIST
                ],
                [
                    "ZRANGEBYSCORE", outboxKey, now, "+inf",
                    "LIMIT", 0, MAX_CHALLENGES_PER_LIST
                ]
            ]);
            const incoming = Array.isArray(incomingIds) ? incomingIds : [];
            const outgoing = Array.isArray(outgoingIds) ? outgoingIds : [];
            const uniqueIds = [...new Set([...incoming, ...outgoing])]
                .map((value) => cleanText(value, 64))
                .filter((value) => /^[a-f0-9]{32}$/.test(value));
            let stored = [];
            if (uniqueIds.length > 0) {
                stored = await upstashPipeline(
                    uniqueIds.map((id) => ["GET", challengeKey(id)])
                );
            }
            const byId = new Map();
            uniqueIds.forEach((id, index) => {
                const parsed = parseStoredJson(stored[index], "challenge record");
                const parsedType = cleanChallengeType(parsed?.type) || "beat_my_time";
                if (parsed && Number(parsed.expires_at || 0) >= now
                    && (requestedType === "all" || parsedType === requestedType)) {
                    byId.set(id, publicChallenge(parsed));
                }
            });
            const newestFirst = (a, b) =>
                Number(b.created_at || 0) - Number(a.created_at || 0);
            const incomingChallenges = incoming
                .map((id) => byId.get(String(id)))
                .filter(Boolean)
                .sort(newestFirst);
            const outgoingChallenges = outgoing
                .map((id) => byId.get(String(id)))
                .filter(Boolean)
                .sort(newestFirst);
            return res.status(200).json({
                success: true,
                server_time: now,
                expires_in: CHALLENGE_TTL_SECONDS,
                incoming: incomingChallenges,
                outgoing: outgoingChallenges
            });
        } catch (error) {
            console.error(
                "[CHALLENGE] List failed:",
                error.response?.status || error.message,
                error.response?.data || ""
            );
            return res.status(503).json({
                success: false,
                incoming: [],
                outgoing: [],
                error: "Challenges are temporarily unavailable."
            });
        }
    }
);
app.post(
    "/challenges/:id/respond",
    requireLinkedIdentity,
    async (req, res) => {
        noStore(res);
        try {
            const challenge = await getChallengeRecord(req.params.id);
            const playerId = req.mdpIdentity.acGuid;
            const action = cleanText(req.body?.action, 20).toLowerCase();
            const now = nowUnixSeconds();
            if (!challenge || Number(challenge.expires_at || 0) <= now) {
                return res.status(404).json({ success: false, error: "Challenge expired or not found." });
            }
            if (challenge.to_player_id !== playerId) {
                return res.status(403).json({ success: false, error: "Only the recipient can respond." });
            }
            if (challenge.status !== "pending") {
                return res.status(409).json({ success: false, error: "Challenge has already been answered." });
            }
            if (action !== "accept" && action !== "decline") {
                return res.status(400).json({ success: false, error: "Choose accept or decline." });
            }
            challenge.status = action === "accept" ? "accepted" : "declined";
            challenge.accepted_at = action === "accept" ? now : 0;
            challenge.terminal_at = action === "decline" ? now : 0;
            const ttl = challengeTtlRemaining(challenge, now);
            const commands = [[
                "SET", challengeKey(challenge.id), JSON.stringify(challenge), "EX", ttl
            ]];
            if (action === "decline") {
                commands.push(["DEL", challengePairKey(challenge.from_player_id, challenge.to_player_id)]);
            }
            await upstashPipeline(commands);
            resetChallengePollGuard(playerId);
            return res.status(200).json({
                success: true,
                server_time: now,
                challenge: publicChallenge(challenge)
            });
        } catch (error) {
            console.error("[CHALLENGE] Response failed:", error.response?.status || error.message);
            return res.status(503).json({ success: false, error: "The response could not be saved." });
        }
    }
);
app.post(
    "/challenges/:id/cancel",
    requireLinkedIdentity,
    async (req, res) => {
        noStore(res);
        try {
            const challenge = await getChallengeRecord(req.params.id);
            const playerId = req.mdpIdentity.acGuid;
            const now = nowUnixSeconds();
            if (!challenge || Number(challenge.expires_at || 0) <= now) {
                return res.status(404).json({ success: false, error: "Challenge expired or not found." });
            }
            if (challenge.from_player_id !== playerId) {
                return res.status(403).json({ success: false, error: "Only the challenger can cancel." });
            }
            if (!["pending", "accepted"].includes(challenge.status)) {
                return res.status(409).json({ success: false, error: "Challenge is already closed." });
            }
            challenge.status = "cancelled";
            challenge.terminal_at = now;
            await upstashPipeline([
                [
                    "SET", challengeKey(challenge.id), JSON.stringify(challenge),
                    "EX", challengeTtlRemaining(challenge, now)
                ],
                ["DEL", challengePairKey(challenge.from_player_id, challenge.to_player_id)]
            ]);
            resetChallengePollGuard(playerId);
            return res.status(200).json({
                success: true,
                server_time: now,
                challenge: publicChallenge(challenge)
            });
        } catch (error) {
            console.error("[CHALLENGE] Cancel failed:", error.response?.status || error.message);
            return res.status(503).json({ success: false, error: "The challenge could not be cancelled." });
        }
    }
);
app.post(
    "/challenges/:id/result",
    requireLinkedIdentity,
    async (req, res) => {
        noStore(res);
        try {
            const challenge = await getChallengeRecord(req.params.id);
            const playerId = req.mdpIdentity.acGuid;
            const now = nowUnixSeconds();
            const elapsedMs = Math.round(Number(req.body?.elapsed_ms));
            const modeKey = cleanModeKey(req.body?.mode_key);
            const resultId = cleanText(req.body?.result_id, 100);
            const completedAt = Math.floor(Number(req.body?.completed_at) || now);
            const valid = req.body?.valid === true;
            if (!challenge || Number(challenge.expires_at || 0) <= now) {
                return res.status(404).json({ success: false, error: "Challenge expired or not found." });
            }
            const challengeType = cleanChallengeType(challenge.type) || "beat_my_time";
            const isParticipant = challenge.from_player_id === playerId
                || challenge.to_player_id === playerId;
            if ((challengeType === "beat_my_time" && challenge.to_player_id !== playerId)
                || (challengeType === "touge" && !isParticipant)) {
                return res.status(403).json({ success: false, error: "Only a challenge participant can submit this result." });
            }
            if (challenge.status !== "accepted") {
                return res.status(409).json({ success: false, error: "Challenge is not active." });
            }
            if (!valid) {
                return res.status(400).json({ success: false, error: "Only valid completed runs are accepted." });
            }
            if (challengeType === "touge") {
                const winnerPlayerId = normalizeAcGuid(req.body?.winner_player_id);
                const winnerRole = cleanText(req.body?.winner_role, 20).toLowerCase();
                const resultReason = cleanText(req.body?.reason, 80).toLowerCase();
                const finalGapM = Number(req.body?.final_gap_m);
                const minGapM = Number(req.body?.min_gap_m);
                const maxGapM = Number(req.body?.max_gap_m);
                const participants = [challenge.from_player_id, challenge.to_player_id];
                const allowedReasons = new Set([
                    "escape_sustained", "duration_survived",
                    "local_reset_or_teleport", "opponent_reset_or_teleport",
                    "opponent_lost_or_disconnected", "local_stopped_forfeit",
                    "opponent_stopped_forfeit"
                ]);
                if (!resultId || !participants.includes(winnerPlayerId)
                    || !["leader", "follower"].includes(winnerRole)
                    || !allowedReasons.has(resultReason)
                    || !Number.isFinite(elapsedMs) || elapsedMs < 0 || elapsedMs > 300000
                    || completedAt + 2 < Number(challenge.accepted_at || 0)) {
                    return res.status(400).json({ success: false, error: "Completed Touge result data is invalid." });
                }
                const resultIds = Array.isArray(challenge.result_ids)
                    ? challenge.result_ids.map((value) => cleanText(value, 100)) : [];
                if (resultIds.includes(resultId)) {
                    return res.status(409).json({
                        success: false,
                        duplicate: true,
                        error: "This Touge result was already submitted."
                    });
                }
                challenge.result_ids = [resultId, ...resultIds].slice(0, 20);
                challenge.status = "completed";
                challenge.completed_at = now;
                challenge.terminal_at = now;
                challenge.winner_player_id = winnerPlayerId;
                challenge.winner_role = winnerRole;
                challenge.result_reason = resultReason;
                challenge.result_elapsed_ms = elapsedMs;
                challenge.final_gap_m = Number.isFinite(finalGapM) ? Math.max(0, finalGapM) : 0;
                challenge.min_gap_m = Number.isFinite(minGapM) ? Math.max(0, minGapM) : 0;
                challenge.max_gap_m = Number.isFinite(maxGapM) ? Math.max(0, maxGapM) : 0;
                const ttl = challengeTtlRemaining(challenge, now);
                await upstashPipeline([
                    ["SET", challengeKey(challenge.id), JSON.stringify(challenge), "EX", ttl],
                    ["DEL", challengePairKey(challenge.from_player_id, challenge.to_player_id)]
                ]);
                resetChallengePollGuard(challenge.from_player_id);
                resetChallengePollGuard(challenge.to_player_id);
                return res.status(200).json({
                    success: true,
                    server_time: now,
                    challenge: publicChallenge(challenge)
                });
            }
            if (!modeKey || modeKey !== challenge.mode_key) {
                return res.status(400).json({ success: false, error: "Completed run does not match the challenge mode." });
            }
            if (!Number.isFinite(elapsedMs) || elapsedMs < 100 || elapsedMs > 3600000 || !resultId) {
                return res.status(400).json({ success: false, error: "Completed result data is invalid." });
            }
            if (completedAt + 2 < Number(challenge.accepted_at || 0)) {
                return res.status(400).json({ success: false, error: "Run was completed before the challenge was accepted." });
            }
            const ttl = challengeTtlRemaining(challenge, now);
            const resultIds = Array.isArray(challenge.result_ids)
                ? challenge.result_ids.map((value) => cleanText(value, 100))
                : [];
            if (resultIds.includes(resultId)) {
                return res.status(409).json({
                    success: false,
                    duplicate: true,
                    error: "This completed run was already submitted."
                });
            }
            challenge.result_ids = [resultId, ...resultIds].slice(0, 20);
            challenge.attempts = Number(challenge.attempts || 0) + 1;
            challenge.last_attempt_ms = elapsedMs;
            challenge.last_attempt_at = completedAt;
            const previousBest = Number(challenge.best_ms || 0);
            challenge.best_ms = previousBest > 0
                ? Math.min(previousBest, elapsedMs)
                : elapsedMs;
            const beaten = elapsedMs < Number(challenge.target_ms || 0);
            if (beaten) {
                challenge.beaten = true;
                challenge.status = "completed";
                challenge.completed_at = now;
                challenge.terminal_at = now;
            }
            const commands = [[
                "SET", challengeKey(challenge.id), JSON.stringify(challenge), "EX", ttl
            ]];
            if (beaten) {
                commands.push(["DEL", challengePairKey(challenge.from_player_id, challenge.to_player_id)]);
            }
            await upstashPipeline(commands);
            resetChallengePollGuard(playerId);
            return res.status(200).json({
                success: true,
                beaten,
                server_time: now,
                challenge: publicChallenge(challenge)
            });
        } catch (error) {
            console.error("[CHALLENGE] Result failed:", error.response?.status || error.message);
            return res.status(503).json({ success: false, error: "The completed result could not be saved." });
        }
    }
);
// This endpoint is passive: it runs only when explicitly requested. It is not
// used as a keep-alive and therefore does not prevent Render from sleeping.
app.get("/health", async (req, res) => {
    noStore(res);
    try {
        const redisReply = await upstashCommand("PING");
        const redisHealthy =
            String(redisReply || "").toUpperCase() === "PONG";
        return res.status(redisHealthy ? 200 : 503).json({
            success: redisHealthy,
            service: "mydragypro-online-presence",
            render: "ok",
            upstash: redisHealthy ? "ok" : "unexpected_response",
            server_time: nowUnixSeconds(),
            presence_ttl_seconds: PRESENCE_TTL_SECONDS,
            heartbeat_seconds: PRESENCE_HEARTBEAT_SECONDS,
            beat_my_time: "enabled",
            challenge_ttl_seconds: CHALLENGE_TTL_SECONDS
        });
    } catch (error) {
        console.error(
            "[HEALTH] Upstash check failed:",
            error.response?.status || error.message
        );
        return res.status(503).json({
            success: false,
            service: "mydragypro-online-presence",
            render: "ok",
            upstash: "unavailable",
            server_time: nowUnixSeconds()
        });
    }
});
// =========================================================
// Sponsored track requests
// =========================================================
app.post(
    "/sponsor-request",
    async (req, res) => {
        try {
            if (!SPONSOR_REQUEST_WEBHOOK_URL) {
                console.error(
                    "SPONSOR_REQUEST_WEBHOOK_URL " +
                    "is not configured."
                );
                return res.status(503).json({
                    success: false,
                    error:
                        "Sponsorship requests are currently unavailable."
                });
            }
            const {
                ac_guid,
                sponsor_key,
                track_identity,
                server_identity,
                track_name,
                track_configuration,
                server_name,
                driver_name,
                app_version
            } = req.body || {};
            const sponsorKey =
                cleanText(sponsor_key, 64);
            const trackIdentity =
                cleanText(track_identity, 200);
            const serverIdentity =
                cleanText(server_identity, 200);
            if (
                !/^[a-fA-F0-9]{64}$/.test(
                    sponsorKey
                )
            ) {
                return res.status(400).json({
                    success: false,
                    error: "Invalid sponsor key."
                });
            }
            if (
                !trackIdentity ||
                !serverIdentity
            ) {
                return res.status(400).json({
                    success: false,
                    error:
                        "Track or server identity is missing."
                });
            }
            const acGuid =
                normalizeAcGuid(ac_guid);
            let discordLink = null;
            let discordVerificationUnavailable =
                false;
            if (acGuid) {
                try {
                    discordLink =
                        await getDiscordLink(
                            acGuid
                        );
                } catch (lookupError) {
                    discordVerificationUnavailable =
                        true;
                    console.error(
                        "[SPONSOR] Discord-link lookup failed:",
                        lookupError.response?.status ||
                        lookupError.message,
                        lookupError.response?.data || ""
                    );
                }
            }
            const discordId =
                discordLink?.discord_id || null;
            console.log(
                `[SPONSOR] Request identity: ` +
                `ac_guid=${JSON.stringify(acGuid)} ` +
                `linked=${Boolean(discordId)} ` +
                `discord_id=${discordId || "none"} ` +
                `source=${discordLink?.source || "none"} ` +
                `verification_unavailable=` +
                `${discordVerificationUnavailable} ` +
                `memory_cache_size=` +
                `${Object.keys(authenticatedUsers).length}`
            );
            let requesterText;
            if (discordId) {
                requesterText =
                    `Linked Discord user: <@${discordId}>`;
            } else if (
                discordVerificationUnavailable
            ) {
                requesterText =
                    "Discord link verification unavailable";
            } else {
                requesterText =
                    "Not linked to Discord";
            }
            const webhookPayload = {
                username:
                    "MyDragyPro Sponsorship Requests",
                allowed_mentions: {
                    parse: []
                },
                embeds: [
                    {
                        title:
                            "ðŸ New Sponsored Track Request",
                        color: 5763719,
                        fields: [
                            {
                                name: "Track",
                                value:
                                    cleanText(
                                        track_name,
                                        200
                                    ) || "Unknown",
                                inline: true
                            },
                            {
                                name: "Configuration",
                                value:
                                    cleanText(
                                        track_configuration,
                                        200
                                    ) || "Default",
                                inline: true
                            },
                            {
                                name: "Server",
                                value:
                                    cleanText(
                                        server_name,
                                        200
                                    ) || "Unknown",
                                inline: false
                            },
                            {
                                name: "Track Identity",
                                value:
                                    `\`${trackIdentity}\``,
                                inline: false
                            },
                            {
                                name: "Server Identity",
                                value:
                                    `\`${serverIdentity}\``,
                                inline: false
                            },
                            {
                                name: "Sponsor Key",
                                value:
                                    `\`${sponsorKey}\``,
                                inline: false
                            },
                            {
                                name: "Requester",
                                value:
                                    requesterText,
                                inline: false
                            },
                            {
                                name: "Driver Name",
                                value:
                                    cleanText(
                                        driver_name,
                                        100
                                    ) || "Unknown",
                                inline: true
                            },
                            {
                                name: "App Version",
                                value:
                                    cleanText(
                                        app_version,
                                        50
                                    ) || "Unknown",
                                inline: true
                            },
                            {
                                name: "Status",
                                value:
                                    "Pending manual review",
                                inline: false
                            }
                        ],
                        timestamp:
                            new Date().toISOString(),
                        footer: {
                            text:
                                "MyDragyPro Sponsorship Request"
                        }
                    }
                ]
            };
            await axios.post(
                SPONSOR_REQUEST_WEBHOOK_URL,
                webhookPayload,
                {
                    headers: {
                        "Content-Type":
                            "application/json"
                    },
                    timeout: 10000
                }
            );
            console.log(
                `Sponsorship request submitted: ` +
                `${sponsorKey}`
            );
            return res.status(200).json({
                success: true,
                message:
                    "Sponsorship request submitted for review."
            });
        } catch (error) {
            const status =
                error.response?.status;
            const details =
                error.response?.data;
            console.error(
                "Sponsorship request failed:",
                status || error.message,
                details || ""
            );
            return res.status(502).json({
                success: false,
                error:
                    "The sponsorship request could not be submitted."
            });
        }
    }
);
// =========================================================
// Server startup
// =========================================================
const PORT =
    Number(process.env.PORT) || 3000;
app.listen(
    PORT,
    () => {
        console.log(
            `OAuth2 server running on port ${PORT}`
        );
        console.log(
            `[REDIS] Upstash configuration: ` +
            `url=${UPSTASH_REDIS_REST_URL ? "set" : "missing"} ` +
            `token=${UPSTASH_REDIS_REST_TOKEN ? "set" : "missing"}`
        );
    }
);
