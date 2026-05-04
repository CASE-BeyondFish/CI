import { NextResponse } from 'next/server';
import { supabase } from '@/lib/db/supabase';

// Read-only sanity check on the handbooks catalog. Useful for confirming
// what's currently mirrored without opening the Supabase dashboard.
// Auth is enforced by middleware (session cookie on /api/admin/*).
export async function GET() {
  const { data, error } = await supabase
    .from('handbooks')
    .select(
      'id, document_short, document_full, fcic_number, document_version, ' +
      'reinsurance_year, storage_path, source_filename, file_size_bytes, ' +
      'rma_source_url, notes, uploaded_at'
    )
    .order('document_short', { ascending: true })
    .order('document_version', { ascending: false });

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json({
    count: data.length,
    handbooks: data,
  });
}
