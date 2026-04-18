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
const PERFORMANCES_SPREADSHEET_ID = "1-dH5HnAQXPfkvr5hfUktCw1I1Bkb5IsVNmiig6Hsst0";
const SCOPES = "https://www.googleapis.com/auth/spreadsheets";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

// ───────────────────────────────────────────
// シート定義
// ───────────────────────────────────────────

const ARTICLES_SHEET = {
  name: "フォームの回答 1",
  range: "A:M",
  columns: [
    "タイムスタンプ", "タイトル", "カテゴリ", "言語", "本文",
    "執筆者名", "公開日", "公演日", "公演時間", "会場", "写真", "公開フラグ", "メールアドレス",
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

const PERFORMANCES_SHEET = {
  name: "Performances",
  range: "A:M",
  columns: [
    "title", "company", "venue", "date", "time",
    "price", "cast", "ticket_on_sale", "official_url", "notes", "published", "source_url", "created_at",
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
          email           = "",
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
          email,
        ];

        const accessToken = await getAccessToken(env);
        const appendResult = await appendSheetRow(accessToken, ARTICLES_SHEET.name, row);
        // updatedRange 例: "フォームの回答 1!A5:M5" → 行番号を抽出
        const updatedRange = appendResult?.updates?.updatedRange ?? "";
        const rowMatch = updatedRange.match(/!A(\d+)/);
        const rowIndex = rowMatch ? parseInt(rowMatch[1], 10) : null;
        return jsonResponse({ success: true, rowIndex });

      } catch (err) {
        console.error(err);
        return jsonResponse({ error: err.message }, 500);
      }
    }

    // ── PUT /articles/:rowIndex ───────────────────────────────────────────────
    const putMatch = pathname.match(/^\/articles\/(\d+)$/);
    if (request.method === "PUT" && putMatch) {
      try {
        const rowIndex = parseInt(putMatch[1], 10);
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
          email           = "",
        } = body;

        const now = new Date();
        const pad = (n) => String(n).padStart(2, "0");
        const timestamp = `${now.getFullYear()}/${pad(now.getMonth() + 1)}/${pad(now.getDate())} ` +
                          `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;

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
          email,
        ];

        const accessToken = await getAccessToken(env);
        await updateSheetRow(accessToken, ARTICLES_SHEET.name, rowIndex, row);
        return jsonResponse({ success: true });

      } catch (err) {
        console.error(err);
        return jsonResponse({ error: err.message }, 500);
      }
    }

    // ── POST /performances/fetch-url ─────────────────────────────────────────
    if (request.method === "POST" && pathname === "/performances/fetch-url") {
      try {
        const body = await request.json();
        const { url } = body;
        if (!url) return jsonResponse({ success: false, error: "url_required" }, 400);
        const controller = new AbortController();
        const timeoutId  = setTimeout(() => controller.abort(), 10000);
        try {
          const res = await fetch(url, {
            signal:  controller.signal,
            headers: { "User-Agent": "Mozilla/5.0 (compatible; DanceTimesBot/1.0)" },
          });
          clearTimeout(timeoutId);
          if (!res.ok) return jsonResponse({ success: false, error: "fetch_failed" });
          const html = await res.text();
          return jsonResponse({ success: true, html: html.substring(0, 50000) });
        } catch {
          clearTimeout(timeoutId);
          return jsonResponse({ success: false, error: "fetch_failed" });
        }
      } catch {
        return jsonResponse({ success: false, error: "fetch_failed" });
      }
    }

    // ── POST /ai/extract ─────────────────────────────────────────────────────
    if (request.method === "POST" && pathname === "/ai/extract") {
      try {
        if (!env.ANTHROPIC_API_KEY) return jsonResponse({ success: false, error: "api_key_not_configured" });
        const body = await request.json();
        const { text, type } = body;
        const userContent = type === "html"
          ? `以下のHTMLから公演情報を抽出してください：\n\n${text.substring(0, 30000)}`
          : `以下のテキストから公演情報を抽出してください：\n\n${text.substring(0, 10000)}`;
        const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
          method:  "POST",
          headers: {
            "x-api-key":         env.ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
          },
          body: JSON.stringify({
            model:      "claude-sonnet-4-20250514",
            max_tokens: 1024,
            system:     'あなたはバレエ・ダンス公演情報の抽出専門家です。与えられたテキストから公演情報を抽出し、必ずJSON形式のみで返してください。出演者情報・スタッフ情報・割引詳細・最新情報は抽出不要です。説明文やMarkdownは一切含めず以下のフォーマットのJSONのみを返してください：{"title":"カンパニー名を含む公演名","company":"カンパニー名・主催","venue":"会場名","date_start":"YYYY-MM-DD","date_end":"YYYY-MM-DD","price":"料金情報（簡潔に）","ticket_on_sale":"YYYY-MM-DD または null","notes":"備考（上演時間・会場変更等）"}',
            messages:   [{ role: "user", content: userContent }],
          }),
        });
        if (!aiRes.ok) return jsonResponse({ success: false, error: "extract_failed" });
        const aiData  = await aiRes.json();
        const rawText = aiData.content?.[0]?.text ?? "";
        let parsed;
        try { parsed = JSON.parse(rawText); } catch { return jsonResponse({ success: false, error: "extract_failed" }); }
        return jsonResponse({ success: true, data: parsed });
      } catch {
        return jsonResponse({ success: false, error: "extract_failed" });
      }
    }

    // ── POST /ai/extract-pdf ─────────────────────────────────────────────────
    if (request.method === "POST" && pathname === "/ai/extract-pdf") {
      try {
        if (!env.ANTHROPIC_API_KEY) return jsonResponse({ success: false, error: "api_key_not_configured" });
        const body = await request.json();
        const { pdf } = body;
        if (!pdf) return jsonResponse({ success: false, error: "pdf_required" }, 400);
        const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
          method:  "POST",
          headers: {
            "x-api-key":         env.ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
          },
          body: JSON.stringify({
            model:      "claude-sonnet-4-20250514",
            max_tokens: 2048,
            system:     'バレエ・ダンス公演フライヤーのPDFから公演情報を抽出し、JSONの配列のみを返してください。複数の公演が含まれる場合はすべて抽出してください。説明文は一切含めず以下のフォーマットで：[{"title":"","company":"","venue":"","date_start":"YYYY-MM-DD","date_end":"YYYY-MM-DD","price":"","ticket_on_sale":"YYYY-MM-DD or null","notes":""}]',
            messages:   [{
              role:    "user",
              content: [{
                type:   "document",
                source: { type: "base64", media_type: "application/pdf", data: pdf },
              }],
            }],
          }),
        });
        if (!aiRes.ok) return jsonResponse({ success: false, error: "extract_failed" });
        const aiData  = await aiRes.json();
        const rawText = aiData.content?.[0]?.text ?? "";
        let parsed;
        try { parsed = JSON.parse(rawText); } catch { return jsonResponse({ success: false, error: "extract_failed" }); }
        return jsonResponse({ success: true, data: parsed });
      } catch {
        return jsonResponse({ success: false, error: "extract_failed" });
      }
    }

    // ── POST /performances ────────────────────────────────────────────────────
    if (request.method === "POST" && pathname === "/performances") {
      try {
        const body = await request.json();
        const {
          title          = "",
          company        = "",
          venue          = "",
          date           = "",
          time           = "",
          price          = "",
          cast           = "",
          ticket_on_sale = "",
          official_url   = "",
          notes          = "",
          published      = "draft",
          source_url     = "",
        } = body;
        const now = new Date();
        const pad = (n) => String(n).padStart(2, "0");
        const created_at = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
        const row = [title, company, venue, date, time, price, cast, ticket_on_sale, official_url, notes, published, source_url, created_at];
        const accessToken  = await getAccessToken(env);
        const appendResult = await appendPerformanceSheetRow(accessToken, PERFORMANCES_SHEET.name, row);
        const updatedRange = appendResult?.updates?.updatedRange ?? "";
        const rowMatch     = updatedRange.match(/!A(\d+)/);
        const rowIndex     = rowMatch ? parseInt(rowMatch[1], 10) : null;
        return jsonResponse({ success: true, rowIndex });
      } catch (err) {
        return jsonResponse({ error: err.message }, 500);
      }
    }

    // ── PUT /performances/:rowIndex ───────────────────────────────────────────
    const perfPutMatch = pathname.match(/^\/performances\/(\d+)$/);
    if (request.method === "PUT" && perfPutMatch) {
      try {
        const rowIndex = parseInt(perfPutMatch[1], 10);
        const body = await request.json();
        const {
          title          = "",
          company        = "",
          venue          = "",
          date           = "",
          time           = "",
          price          = "",
          cast           = "",
          ticket_on_sale = "",
          official_url   = "",
          notes          = "",
          published      = "draft",
          source_url     = "",
          created_at     = "",
        } = body;
        const row         = [title, company, venue, date, time, price, cast, ticket_on_sale, official_url, notes, published, source_url, created_at];
        const accessToken = await getAccessToken(env);
        await updatePerformanceSheetRow(accessToken, PERFORMANCES_SHEET.name, rowIndex, row);
        return jsonResponse({ success: true });
      } catch (err) {
        return jsonResponse({ error: err.message }, 500);
      }
    }

    // ── DELETE /performances/:rowIndex ────────────────────────────────────────
    const perfDeleteMatch = pathname.match(/^\/performances\/(\d+)$/);
    if (request.method === "DELETE" && perfDeleteMatch) {
      try {
        const rowIndex    = parseInt(perfDeleteMatch[1], 10);
        const accessToken = await getAccessToken(env);
        await deleteSheetRow(accessToken, PERFORMANCES_SHEET.name, rowIndex);
        return jsonResponse({ success: true });
      } catch (err) {
        return jsonResponse({ error: err.message }, 500);
      }
    }

    // ── GET only below ────────────────────────────────────────────────────────
    if (request.method !== "GET") {
      return jsonResponse({ error: "Method not allowed" }, 405);
    }

    try {
      const accessToken = await getAccessToken(env);

      if (pathname === "/articles/search") {
        const url = new URL(request.url);
        const q           = (url.searchParams.get("q")           ?? "").trim().toLowerCase();
        const authorEmail = (url.searchParams.get("authorEmail") ?? "").trim().toLowerCase();
        const rows = await fetchSheetData(accessToken, ARTICLES_SHEET);
        const results = searchArticleRows(rows, q, authorEmail);
        return jsonResponse(results);
      }

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

      if (pathname === "/performances/all") {
        const rows = await fetchPerformanceSheetData(accessToken, PERFORMANCES_SHEET);
        return jsonResponse(parsePerformanceRows(rows, { includeAll: true }));
      }

      if (pathname === "/performances/month-list") {
        const rows  = await fetchPerformanceSheetData(accessToken, PERFORMANCES_SHEET);
        const perfs = parsePerformanceRows(rows);
        const months = [...new Set(
          perfs
            .map(p => p.date ? p.date.slice(0, 7) : null)
            .filter(Boolean)
        )].sort();
        return jsonResponse(months);
      }

      if (pathname === "/performances") {
        const perfUrl    = new URL(request.url);
        const monthParam = perfUrl.searchParams.get("month");
        const printParam = perfUrl.searchParams.get("print") === "true";
        const rows       = await fetchPerformanceSheetData(accessToken, PERFORMANCES_SHEET);
        let   perfs      = parsePerformanceRows(rows, { print: printParam });

        if (monthParam) {
          perfs = perfs.filter(p => p.date && p.date.slice(0, 7) === monthParam);
        } else {
          const now    = new Date();
          const pad2   = (n) => String(n).padStart(2, "0");
          const today  = `${now.getFullYear()}-${pad2(now.getMonth()+1)}-${pad2(now.getDate())}`;
          const endD   = new Date(now.getFullYear(), now.getMonth() + 3, now.getDate());
          const endStr = `${endD.getFullYear()}-${pad2(endD.getMonth()+1)}-${pad2(endD.getDate())}`;
          perfs = perfs.filter(p => p.date && p.date >= today && p.date <= endStr);
        }

        return jsonResponse(perfs);
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

/** キーワード検索（タイトル・著者・カテゴリ対象、最大20件、rowIndex付き）
 *  authorEmail が指定された場合はメールアドレス列でさらに絞り込む */
function searchArticleRows(rows, q, authorEmail = "") {
  if (rows.length < 2) return [];

  const colIndex = buildColIndex(rows[0], ARTICLES_SHEET.columns);
  const get = (row, col) => {
    const idx = colIndex[col];
    return idx >= 0 ? (row[idx] ?? "") : "";
  };

  const results = [];
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const flag = get(row, "公開フラグ");
    if (flag !== "published" && flag !== "draft") continue;

    // authorEmail フィルタ（指定がある場合のみ）
    if (authorEmail !== "") {
      const rowEmail = get(row, "メールアドレス").trim().toLowerCase();
      if (rowEmail !== authorEmail) continue;
    }

    const title    = get(row, "タイトル");
    const author   = get(row, "執筆者名");
    const category = get(row, "カテゴリ");

    if (
      q === "" ||
      title.toLowerCase().includes(q) ||
      author.toLowerCase().includes(q) ||
      category.toLowerCase().includes(q)
    ) {
      results.push({
        rowIndex:        i + 1, // 1-based: header=1, first data row=2
        timestamp:       get(row, "タイムスタンプ"),
        title,
        category,
        language:        get(row, "言語"),
        body:            get(row, "本文"),
        author,
        publishDate:     get(row, "公開日"),
        performanceDate: get(row, "公演日"),
        performanceTime: get(row, "公演時間"),
        venue:           get(row, "会場"),
        photo:           get(row, "写真"),
        publishFlag:     get(row, "公開フラグ"),
      });
    }

    if (results.length >= 20) break;
  }
  return results;
}

async function updateSheetRow(accessToken, sheetName, rowIndex, row) {
  // rowIndex は1始まり（Sheets API の行番号と同じ）
  const range = encodeURIComponent(`${sheetName}!A${rowIndex}`);
  const url   = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${range}` +
                `?valueInputOption=USER_ENTERED`;

  const res = await fetch(url, {
    method:  "PUT",
    headers: {
      Authorization:  `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ values: [row] }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Sheets update error (${sheetName} row ${rowIndex}): ${res.status} ${body}`);
  }

  return res.json();
}

// ───────────────────────────────────────────
// Performances Sheets API（別スプレッドシート）
// ───────────────────────────────────────────

async function fetchPerformanceSheetData(accessToken, sheet) {
  const range = encodeURIComponent(`${sheet.name}!${sheet.range}`);
  const url   = `https://sheets.googleapis.com/v4/spreadsheets/${PERFORMANCES_SPREADSHEET_ID}/values/${range}`;
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

async function appendPerformanceSheetRow(accessToken, sheetName, row) {
  const range = encodeURIComponent(`${sheetName}!A1`);
  const url   = `https://sheets.googleapis.com/v4/spreadsheets/${PERFORMANCES_SPREADSHEET_ID}/values/${range}:append` +
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

async function updatePerformanceSheetRow(accessToken, sheetName, rowIndex, row) {
  const range = encodeURIComponent(`${sheetName}!A${rowIndex}`);
  const url   = `https://sheets.googleapis.com/v4/spreadsheets/${PERFORMANCES_SPREADSHEET_ID}/values/${range}` +
                `?valueInputOption=USER_ENTERED`;
  const res = await fetch(url, {
    method:  "PUT",
    headers: {
      Authorization:  `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ values: [row] }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Sheets update error (${sheetName} row ${rowIndex}): ${res.status} ${body}`);
  }
  return res.json();
}

// ───────────────────────────────────────────
// Performances helpers
// ───────────────────────────────────────────

function parsePerformanceRows(rows, options = {}) {
  if (rows.length < 2) return [];
  const { includeAll = false, print = false } = options;

  const colIndex = buildColIndex(rows[0], PERFORMANCES_SHEET.columns);
  const get = (row, col) => {
    const idx = colIndex[col];
    return idx >= 0 ? (row[idx] ?? "") : "";
  };

  const perfs = [];
  for (let i = 1; i < rows.length; i++) {
    const row       = rows[i];
    const published = get(row, "published");
    if (!includeAll && published !== "published") continue;

    const obj = {
      title:   get(row, "title"),
      company: get(row, "company"),
      venue:   get(row, "venue"),
      date:    get(row, "date"),
      time:    get(row, "time"),
      price:   get(row, "price"),
      cast:    get(row, "cast"),
      notes:   get(row, "notes"),
      published,
    };

    if (!print) {
      obj.ticket_on_sale = get(row, "ticket_on_sale");
      obj.official_url   = get(row, "official_url");
    }

    if (includeAll) {
      obj.source_url = get(row, "source_url");
      obj.created_at = get(row, "created_at");
      obj.rowIndex   = i + 1; // 1-based: header=1, first data row=2
    }

    perfs.push(obj);
  }
  return perfs;
}

async function getSheetId(accessToken, sheetName) {
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${PERFORMANCES_SPREADSHEET_ID}?fields=sheets.properties`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Sheets metadata error: ${res.status} ${body}`);
  }
  const data  = await res.json();
  const sheet = (data.sheets ?? []).find(s => s.properties.title === sheetName);
  if (!sheet) throw new Error(`Sheet "${sheetName}" not found`);
  return sheet.properties.sheetId;
}

async function deleteSheetRow(accessToken, sheetName, rowIndex) {
  const sheetId    = await getSheetId(accessToken, sheetName);
  const startIndex = rowIndex - 1; // 0-based for API
  const url        = `https://sheets.googleapis.com/v4/spreadsheets/${PERFORMANCES_SPREADSHEET_ID}:batchUpdate`;
  const res = await fetch(url, {
    method:  "POST",
    headers: {
      Authorization:  `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      requests: [{
        deleteDimension: {
          range: {
            sheetId,
            dimension:  "ROWS",
            startIndex,
            endIndex:   startIndex + 1,
          },
        },
      }],
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Sheets delete error (row ${rowIndex}): ${res.status} ${body}`);
  }
  return res.json();
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      ...CORS_HEADERS,
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}
