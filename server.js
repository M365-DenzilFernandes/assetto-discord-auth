import express from "express";
import axios from "axios";

const app = express();

const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const REDIRECT_URI = process.env.REDIRECT_URI;

let authenticatedUsers = {}; // AC_GUID → Discord_ID

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
        { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );

    const userRes = await axios.get("https://discord.com/api/users/@me", {
        headers: { Authorization: `Bearer ${tokenRes.data.access_token}` }
    });

    authenticatedUsers[ac_guid] = userRes.data.id;

    res.send("Authentication complete. You can close this window.");
});

app.get("/check", (req, res) => {
    const ac_guid = req.query.ac_guid;
    const discordId = authenticatedUsers[ac_guid];

    res.json({
        authenticated: !!discordId,
        discord_id: discordId || null
    });
});

app.listen(3000, () => console.log("OAuth2 server running"));
