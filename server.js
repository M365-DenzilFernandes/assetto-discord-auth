import express from "express";
import axios from "axios";

const app = express();

// Required only for the new sponsorship POST request.
// Existing GET routes continue working exactly as before.
app.use(express.json({ limit: "8kb" }));

const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const REDIRECT_URI = process.env.REDIRECT_URI;

// New Render environment variable.
// Keep the actual Discord webhook out of this source file.
const SPONSOR_REQUEST_WEBHOOK_URL =
    process.env.SPONSOR_REQUEST_WEBHOOK_URL;

let authenticatedUsers = {}; // AC_GUID → Discord_ID

// =========================================================
// EXISTING DISCORD AUTHENTICATION
// =========================================================

app.get("/login", (req, res) => {
    const ac_guid = req.query.ac_guid;

    const url =
        `https://discord.com/oauth2/authorize?client_id=${CLIENT_ID}` +
        `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
        `&response_type=code&scope=identify` +
        `&state=${ac_guid}`;

    res.redirect(url);
});

app.get("/callback", async (req, res) => {
    const code = req.query.code;
    const ac_guid = req.query.state;

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
                "Content-Type": "application/x-www-form-urlencoded"
            }
        }
    );

    const userRes = await axios.get(
        "https://discord.com/api/users/@me",
        {
            headers: {
                Authorization: `Bearer ${tokenRes.data.access_token}`
            }
        }
    );

    authenticatedUsers[ac_guid] = userRes.data.id;
    console.log(
        `[AUTH] OAuth link stored: ` +
        `ac_guid=${JSON.stringify(ac_guid)} ` +
        `discord_id=${userRes.data.id}`
    );
    res.send(
        "✅ MyDragyPro Authentication Complete. " +
        "You can close this window. ❌"
    );
});

app.get("/check", (req, res) => {
    const ac_guid = req.query.ac_guid;
    const discordId = authenticatedUsers[ac_guid];

    res.json({
        authenticated: !!discordId,
        discord_id: discordId || null
    });
});

// =========================================================
// SPONSORED TRACK REQUESTS
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

app.post("/sponsor-request", async (req, res) => {
    try {
        if (!SPONSOR_REQUEST_WEBHOOK_URL) {
            console.error(
                "SPONSOR_REQUEST_WEBHOOK_URL is not configured."
            );

            return res.status(503).json({
                success: false,
                error: "Sponsorship requests are currently unavailable."
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

        const sponsorKey = cleanText(sponsor_key, 64);
        const trackIdentity = cleanText(track_identity, 200);
        const serverIdentity = cleanText(server_identity, 200);

        if (!/^[a-fA-F0-9]{64}$/.test(sponsorKey)) {
            return res.status(400).json({
                success: false,
                error: "Invalid sponsor key."
            });
        }

        if (!trackIdentity || !serverIdentity) {
            return res.status(400).json({
                success: false,
                error: "Track or server identity is missing."
            });
        }

        const acGuid = cleanText(ac_guid, 100);

        // Discord linking is optional.
        // An unlinked user is still allowed to submit.
        const discordId =
            acGuid && authenticatedUsers[acGuid]
                ? authenticatedUsers[acGuid]
                : null;
        
        console.log(
            `[SPONSOR] Request identity: ` +
            `ac_guid=${JSON.stringify(acGuid)} ` +
            `linked=${Boolean(discordId)} ` +
            `discord_id=${discordId || "none"} ` +
            `stored_links=${Object.keys(authenticatedUsers).length}`
        );
        
        const requesterText = discordId
            ? `Linked Discord user: <@${discordId}>`
            : "Not linked to Discord";

        const webhookPayload = {
            username: "MyDragyPro Sponsorship Requests",
            allowed_mentions: {
                parse: []
            },
            embeds: [
                {
                    title: "🏁 New Sponsored Track Request",
                    color: 5763719,
                    fields: [
                        {
                            name: "Track",
                            value:
                                cleanText(track_name, 200) ||
                                "Unknown",
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
                                cleanText(server_name, 200) ||
                                "Unknown",
                            inline: false
                        },
                        {
                            name: "Track Identity",
                            value: `\`${trackIdentity}\``,
                            inline: false
                        },
                        {
                            name: "Server Identity",
                            value: `\`${serverIdentity}\``,
                            inline: false
                        },
                        {
                            name: "Sponsor Key",
                            value: `\`${sponsorKey}\``,
                            inline: false
                        },
                        {
                            name: "Requester",
                            value: requesterText,
                            inline: false
                        },
                        {
                            name: "Driver Name",
                            value:
                                cleanText(driver_name, 100) ||
                                "Unknown",
                            inline: true
                        },
                        {
                            name: "App Version",
                            value:
                                cleanText(app_version, 50) ||
                                "Unknown",
                            inline: true
                        },
                        {
                            name: "Status",
                            value: "Pending manual review",
                            inline: false
                        }
                    ],
                    timestamp: new Date().toISOString(),
                    footer: {
                        text: "MyDragyPro Sponsorship Request"
                    }
                }
            ]
        };

        await axios.post(
            SPONSOR_REQUEST_WEBHOOK_URL,
            webhookPayload,
            {
                headers: {
                    "Content-Type": "application/json"
                },
                timeout: 10000
            }
        );

        console.log(
            `Sponsorship request submitted: ${sponsorKey}`
        );

        return res.status(200).json({
            success: true,
            message: "Sponsorship request submitted for review."
        });
    } catch (error) {
        const status = error.response?.status;
        const details = error.response?.data;

        console.error(
            "Sponsorship request failed:",
            status || error.message,
            details || ""
        );

        return res.status(502).json({
            success: false,
            error: "The sponsorship request could not be submitted."
        });
    }
});

// =========================================================
// SERVER STARTUP
// =========================================================

// Intentionally unchanged to avoid affecting the production service.
app.listen(3000, () =>
    console.log("OAuth2 server running")
);
