// A stand-in for the payments app's POST /api/internal/settle, so the accepted
// consumer can be tested locally without running the real Next.js app on Vercel.
//
// It answers PROCESSED for every signal, EXCEPT as a testing hook it reads the
// customerId to force the other outcomes:
//   customerId contains "fail_user"   -> PENDING + USER_ERROR   (terminal Failed)
//   customerId contains "fail_server" -> PENDING + SERVER_ERROR (retry)
//
// Runs as a tiny container on the compose network; the Lambda reaches it at
// http://mock-settle:3999. No auth check — it trusts everything, which a mock
// may and the real route may not.
import { createServer } from 'node:http'

const PORT = Number.parseInt(process.env.PORT ?? '3999', 10)

function resultFor(signal) {
  const customer = String(signal.customerId ?? '')
  if (customer.includes('fail_user')) {
    return {
      signal_id: signal.signalId,
      status: 'PENDING',
      error_type: 'USER_ERROR',
      error_code: 'NO_ACTIVE_PLAN',
    }
  }
  if (customer.includes('fail_server')) {
    return {
      signal_id: signal.signalId,
      status: 'PENDING',
      error_type: 'SERVER_ERROR',
      error_code: 'SETTLE_TIMEOUT',
    }
  }
  return { signal_id: signal.signalId, status: 'PROCESSED', error_type: null, error_code: null }
}

createServer((req, res) => {
  if (req.method !== 'POST' || !req.url?.startsWith('/api/internal/settle')) {
    res.writeHead(404).end()
    return
  }
  let body = ''
  req.on('data', (chunk) => {
    body += chunk
  })
  req.on('end', () => {
    let parsed = {}
    try {
      parsed = JSON.parse(body)
    } catch {
      /* empty */
    }
    const signals = Array.isArray(parsed.signals) ? parsed.signals : []
    const results = signals.map(resultFor)
    const processed = results.filter((r) => r.status === 'PROCESSED').length
    res.writeHead(200, { 'content-type': 'application/json' }).end(
      JSON.stringify({
        statusCode: 200,
        statusDetail: { status: 'SUCCESS', message: 'settled (mock)' },
        result: {
          batchId: parsed.batchId ?? '',
          total: results.length,
          processed,
          pending: results.length - processed,
          signals: results,
        },
      }),
    )
  })
}).listen(PORT, () => console.log(`mock-settle listening on :${PORT}`))
