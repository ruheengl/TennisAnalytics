const API_BASE = '/api'

async function parseResponse(response, context) {
  if (!response.ok) {
    const body = await response.text()
    throw new Error(body || `${context} failed (${response.status})`)
  }
  return response.json()
}

export async function apiGet(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`, window.location.origin)
  Object.entries(params).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== '') {
      url.searchParams.set(key, value)
    }
  })

  const response = await fetch(url)
  return parseResponse(response, `GET ${path}`)
}

export async function apiPost(path, payload) {
  const response = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  })

  return parseResponse(response, `POST ${path}`)
}
