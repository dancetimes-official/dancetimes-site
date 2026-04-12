/**
 * Cloudflare Worker: Google Sheets -> JSON API
 *
 * Environment variables (set via wrangler secret or dashboard):
 *   GOOGLE_SERVICE_ACCOUNT_EMAIL  - サービスアカウントのメールアドレス
 *   GOOGLE_PRIVATE_KEY            - PEM形式の秘密鍵（-----BEGIN PRIVATE KEY----- ... -----END PRIVATE KEY-----）
 *
 * Endpoints:
 *   GET /articles  - フォームの回答 1 シートから公開フラグ=published の記事を返す
 *   GET /archive   - Archive シートの全記事を返す
 *   GET /all       - 上記2つを結合して返す（source フィールド付き）
 */

const SPREADSHEET_ID = "1Y-aDLAMwD-OTW6vfAr0oRUTdKVlddOWNaiNU_O2M9y0";
const SCOPES = "https://www.googleapis.com/auth/spreadsheets";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

// ───────────────────────────────────────────
// シート定義
// ───────────────────────────────────────────

const ARTICLES_SHEET = {
  name: "フォームの回答 1",
  range: "A:L",
  columns: [
    "タイムスタンプ", "タイトル", "カテゴリ", "言語", "本文",
    "執筆者名", "公開日", "公演日", "公演時間", "会場", "写真", "公開フラグ",
  ],
};

const ARCHIVE_SHEET = {
  name: "Archive",
  range: "A:K",
  columns: [
    "id", "title", "category", "body", "author",
    "original_date", "performance_date", "performance_time",
    "venue", "original_path", "photo_url",
  ],
};

// ───────────────────────────────────────────
// ルーティング
// ───────────────────────────────────────────

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const { pathname } = new URL(request.url);

    // ── POST /articles ────────────────────────────────────────────────────────
    if (request.method === "POST" && pathname === "/articles") {
      try {
        const body = await request.json();
        const {
          title           = "",
          category        = "",
          language        = "",
          body: articleBody = "",
          author          = "",
          publishDate     = "",
          performanceDate = "",
          performanceTime = "",
          venue           = "",
          photoUrl        = "",
          status          = "draft",
        } = body;

        // タイムスタンプ（Google Forms 形式: YYYY/MM/DD HH:MM:SS）
        const now = new Date();
        const pad = (n) => String(n).padStart(2, "0");
        const timestamp = `${now.getFullYear()}/${pad(now.getMonth() + 1)}/${pad(now.getDate())} ` +
                          `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;

        // シートの列順: タイムスタンプ / タイトル / カテゴリ / 言語 / 本文 /
        //               執筆者名 / 公開日 / 公演日 / 公演時間 / 会場 / 写真 / 公開フラグ / メールアドレス
        const row = [
          timestamp,
          title,
          category,
          language,
          articleBody,
          author,
          publishDate,
          performanceDate,
          performanceTime,
          venue,
          photoUrl,
          status,
          "",              // メールアドレス（空白）
        ];

        const accessToken = await getAccessToken(env);
        await appendSheetRow(accessToken, ARTICLES_SHEET.name, row);
        return jsonResponse({ success: true });

      } catch (err) {
        console.error(err);
        return jsonResponse({ error: err.message }, 500);
      }
    }

    // ── GET only below ────────────────────────────────────────────────────────
    if (request.method !== "GET") {
      return jsonResponse({ error: "Method not allowed" }, 405);
    }

    try {
      const accessToken = await getAccessToken(env);

      if (pathname === "/archive") {
        const rows = await fetchSheetData(accessToken, ARCHIVE_SHEET);
        return jsonResponse(parseArchiveRows(rows));
      }

      if (pathname === "/all") {
        const [articlesRows, archiveRows] = await Promise.all([
          fetchSheetData(accessToken, ARTICLES_SHEET),
          fetchSheetData(accessToken, ARCHIVE_SHEET),
        ]);
        const articles = parseArticleRows(articlesRows).map((a) => ({ ...a, source: "articles" }));
        const archive  = parseArchiveRows(archiveRows).map((a)  => ({ ...a, source: "archive" }));
        return jsonResponse([...articles, ...archive]);
      }

      // デフォルト: /articles（既存の挙動と互換）
      const rows = await fetchSheetData(accessToken, ARTICLES_SHEET);
      return jsonResponse(parseArticleRows(rows));

    } catch (err) {
      console.error(err);
      return jsonResponse({ error: err.message }, 500);
    }
  },
};

// ───────────────────────────────────────────
// JWT / OAuth2
// ───────────────────────────────────────────

async function getAccessToken(env) {
  const email  = env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const pemKey = env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, "\n");

  if (!email || !pemKey) {
    throw new Error(
      "GOOGLE_SERVICE_ACCOUNT_EMAIL または GOOGLE_PRIVATE_KEY が設定されていません"
    );
  }

  const now     = Math.floor(Date.now() / 1000);
  const header  = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss:   email,
    scope: SCOPES,
    aud:   "https://oauth2.googleapis.com/token",
    iat:   now,
    exp:   now + 3600,
  };

  const jwt = await signJwt(header, payload, pemKey);

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method:  "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body:    new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion:  jwt,
    }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`OAuth2 token error: ${res.status} ${body}`);
  }

  const data = await res.json();
  return data.access_token;
}

async function signJwt(header, payload, pemKey) {
  const encode = (obj) =>
    btoa(JSON.stringify(obj))
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/, "");

  const signingInput = `${encode(header)}.${encode(payload)}`;
  const cryptoKey    = await importPrivateKey(pemKey);
  const signature    = await crypto.subtle.sign(
    { name: "RSASSA-PKCS1-v1_5" },
    cryptoKey,
    new TextEncoder().encode(signingInput)
  );

  const b64sig = btoa(String.fromCharCode(...new Uint8Array(signature)))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");

  return `${signingInput}.${b64sig}`;
}

async function importPrivateKey(pem) {
  const pemBody = pem
    .replace(/-----BEGIN PRIVATE KEY-----/, "")
    .replace(/-----END PRIVATE KEY-----/, "")
    .replace(/\s+/g, "");

  const der = Uint8Array.from(atob(pemBody), (c) => c.charCodeAt(0));

  return crypto.subtle.importKey(
    "pkcs8",
    der.buffer,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"]
  );
}

// ───────────────────────────────────────────
// Google Sheets API
// ───────────────────────────────────────────

async function appendSheetRow(accessToken, sheetName, row) {
  const range = encodeURIComponent(`${sheetName}!A1`);
  const url   = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${range}:append` +
                `?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`;

  const res = await fetch(url, {
    method:  "POST",
    headers: {
      Authorization:  `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ values: [row] }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Sheets append error (${sheetName}): ${res.status} ${body}`);
  }

  return res.json();
}

async function fetchSheetData(accessToken, sheet) {
  const range = encodeURIComponent(`${sheet.name}!${sheet.range}`);
  const url   = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${range}`;

  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Sheets API error (${sheet.name}): ${res.status} ${body}`);
  }

  const data = await res.json();
  return data.values ?? [];
}

// ───────────────────────────────────────────
// 行データ → オブジェクト変換
// ───────────────────────────────────────────

/** ヘッダー行から列名→インデックスのマップを作る共通ヘルパー */
function buildColIndex(headers, columns) {
  const colIndex = {};
  for (const col of columns) {
    colIndex[col] = headers.indexOf(col);
  }
  return colIndex;
}

/** フォームの回答 1 シート（公開フラグ=published のみ返す） */
function parseArticleRows(rows) {
  if (rows.length < 2) return [];

  const colIndex = buildColIndex(rows[0], ARTICLES_SHEET.columns);
  const get = (row, col) => {
    const idx = colIndex[col];
    return idx >= 0 ? (row[idx] ?? "") : "";
  };

  const articles = [];
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    if (get(row, "公開フラグ") !== "published") continue;

    articles.push({
      timestamp:       get(row, "タイムスタンプ"),
      title:           get(row, "タイトル"),
      category:        get(row, "カテゴリ"),
      language:        get(row, "言語"),
      body:            get(row, "本文"),
      author:          get(row, "執筆者名"),
      publishDate:     get(row, "公開日"),
      performanceDate: get(row, "公演日"),
      performanceTime: get(row, "公演時間"),
      venue:           get(row, "会場"),
      photo:           get(row, "写真"),
      publishFlag:     get(row, "公開フラグ"),
    });
  }
  return articles;
}

/** Archive シート（全行を返す） */
function parseArchiveRows(rows) {
  if (rows.length < 2) return [];

  const colIndex = buildColIndex(rows[0], ARCHIVE_SHEET.columns);
  const get = (row, col) => {
    const idx = colIndex[col];
    return idx >= 0 ? (row[idx] ?? "") : "";
  };

  const archive = [];
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    archive.push({
      id:              get(row, "id"),
      title:           get(row, "title"),
      category:        get(row, "category"),
      body:            get(row, "body"),
      author:          get(row, "author"),
      originalDate:    get(row, "original_date"),
      performanceDate: get(row, "performance_date"),
      performanceTime: get(row, "performance_time"),
      venue:           get(row, "venue"),
      originalPath:    get(row, "original_path"),
      photoUrl:        get(row, "photo_url"),
    });
  }
  return archive;
}

// ───────────────────────────────────────────
// ユーティリティ
// ───────────────────────────────────────────

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      ...CORS_HEADERS,
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}
