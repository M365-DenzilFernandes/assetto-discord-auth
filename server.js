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

        return res.send(
            "✅ MyDragyPro Authentication Complete. " +
            "You can close this window. ❌"
        );
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

        return res.status(502).send(
            "❌ MyDragyPro Authentication Failed. " +
            "Please close this window and try again."
        );
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
