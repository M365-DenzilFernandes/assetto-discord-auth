import express from "express";
import axios from "axios";

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
 const icon = success ? "✓" : "!";
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

        try {
            const requesterGuid =
                req.mdpIdentity.acGuid;
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
                storedRecords = await upstashPipeline(
                    playerIds.map((playerId) => [
                        "GET",
                        presenceKeyForAcGuid(playerId)
                    ])
                );
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
            heartbeat_seconds: PRESENCE_HEARTBEAT_SECONDS
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
                            "🏁 New Sponsored Track Request",

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
