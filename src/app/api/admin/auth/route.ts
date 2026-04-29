import { NextRequest, NextResponse } from 'next/server';
import { setSessionCookie } from '@/lib/auth/session';

export async function POST(request: NextRequest) {
  try {
    const { password } = await request.json();

    const secret = process.env.ADMIN_SECRET;
    if (!secret || secret === 'changeme') {
      return NextResponse.json(
        { error: 'ADMIN_SECRET not configured' },
        { status: 500 }
      );
    }

    if (password !== secret) {
      return NextResponse.json(
        { error: 'Invalid password' },
        { status: 401 }
      );
    }

    const response = NextResponse.json({ success: true });
    await setSessionCookie(response, secret);
    return response;
  } catch {
    return NextResponse.json(
      { error: 'Invalid request' },
      { status: 400 }
    );
  }
}
