async function parseApiResponse(res, hint) {
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text || "{}");
  } catch (_err) {
    throw new Error(`${hint} failed with non-JSON response`);
  }
  if (!res.ok || !data.ok) {
    throw new Error(data.error || `${hint} failed`);
  }
  return data;
}

export async function apiGet(path) {
  const res = await fetch(path, { cache: "no-store" });
  return parseApiResponse(res, `GET ${path}`);
}

export async function apiPost(path, payload) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  return parseApiResponse(res, `POST ${path}`);
}
