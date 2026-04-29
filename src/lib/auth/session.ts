import { NextRequest, NextResponse } from 'next/server';

const COOKIE_NAME = 'cropiq-admin';
const MAX_AGE_SECONDS = 60 * 60 * 24; // 24 hours

function getSecret(): string {
  const secret = process.env.ADMIN_SECRET;
  if (!secret || secret === 'changeme') {
    throw new Error('ADMIN_SECRET must be set to a real value');
  }
  return secret;
}

async function hmacSHA256(message: string, secret: string): Promise<string> {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const signature = await crypto.subtle.sign('HMAC', key, encoder.encode(message));
  return Array.from(new Uint8Array(signature))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

export async function createSessionToken(secret: string): Promise<string> {
  const timestamp = Date.now().toString();
  const hmac = await hmacSHA256(timestamp, secret);
  return `${timestamp}.${hmac}`;
}

export async function validateSessionToken(token: string, secret: string): Promise<boolean> {
  const [timestamp, hmac] = token.split('.');
  if (!timestamp || !hmac) return false;

  // Check expiration
  const age = Date.now() - parseInt(timestamp, 10);
  if (age > MAX_AGE_SECONDS * 1000) return false;

  // Verify HMAC
  const expected = await hmacSHA256(timestamp, secret);
  if (expected.length !== hmac.length) return false;

  // Constant-time comparison
  let mismatch = 0;
  for (let i = 0; i < expected.length; i++) {
    mismatch |= expected.charCodeAt(i) ^ hmac.charCodeAt(i);
  }
  return mismatch === 0;
}

export async function setSessionCookie(response: NextResponse, secret: string): Promise<void> {
  const token = await createSessionToken(secret);
  response.cookies.set(COOKIE_NAME, token, {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: MAX_AGE_SECONDS,
    path: '/',
  });
}

export async function getSessionFromRequest(request: NextRequest): Promise<boolean> {
  const token = request.cookies.get(COOKIE_NAME)?.value;
  if (!token) return false;

  try {
    return await validateSessionToken(token, getSecret());
  } catch {
    return false;
  }
}

export { COOKIE_NAME };
