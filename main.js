const express = require('express')
const { createServer } = require('http')
const { Server } = require('socket.io')
const session = require('express-session')
const cors = require('cors')
const { router: apiRouter, getBracket, setIo } = require('./routes/api')
const { tournaments, registrations, matchWinners } = require('./store')

const app = express()
const httpServer = createServer(app)
const PORT = process.env.PORT || 4000

const sessionMiddleware = session({
  secret: 'pwp_session_secret',
  resave: false,
  saveUninitialized: false,
  cookie: { secure: false, httpOnly: true, maxAge: 24 * 60 * 60 * 1000 },
})

app.use(cors({ origin: 'http://localhost:5173', credentials: true }))
app.use(express.json())
app.use(sessionMiddleware)
app.use('/api', apiRouter)

app.get('/', (req, res) => res.json({ status: 'ok', message: 'PWP backend is running' }))
app.use((req, res) => res.status(404).json({ error: 'Not Found' }))

const io = new Server(httpServer, {
  cors: { origin: 'http://localhost:5173', credentials: true },
})

io.use((socket, next) => sessionMiddleware(socket.request, socket.request.res || {}, next))
setIo(io)

const MATCH_TIMEOUT_MS = 2 * 60 * 1000 // 2 minutes

// matchLobby[roomId] = { players: Set<string>, readyPlayers: Set<string>, timerStartedAt: number|null, timer: Timeout|null }
const matchLobby = {}

function broadcastBracket(tournamentId) {
  const tournament = tournaments.find((t) => String(t.id) === String(tournamentId))
  if (!tournament) return
  const bracketData = getBracket(tournamentId, tournament)
  io.to(`tournament_${tournamentId}`).emit('bracket_updated', { bracket: bracketData.rounds || [] })
}

function handleAutoWin(tournamentId, matchId, winnerName) {
  if (!matchWinners[tournamentId]) matchWinners[tournamentId] = {}
  matchWinners[tournamentId][matchId] = winnerName

  const roomId = `t${tournamentId}_m${matchId}`
  delete matchLobby[roomId]

  io.to(`tournament_${tournamentId}`).emit('match_result', {
    matchId,
    winner: winnerName,
    reason: 'timeout',
  })

  broadcastBracket(tournamentId)
}

io.on('connection', (socket) => {
  // Join the tournament-level room for bracket updates
  socket.on('join_tournament', ({ tournamentId, playerName }) => {
    if (!tournamentId) return
    socket.join(`tournament_${tournamentId}`)

    // Send current bracket immediately
    const tournament = tournaments.find((t) => String(t.id) === String(tournamentId))
    if (tournament) {
      const bracketData = getBracket(tournamentId, tournament)
      socket.emit('bracket_updated', { bracket: bracketData.rounds || [] })
    }
  })

  // Join match-specific lobby — timer starts when both players are present
  socket.on('join_match_lobby', ({ tournamentId, matchId, playerName }) => {
    if (!tournamentId || !matchId || !playerName) return
    const roomId = `t${tournamentId}_m${matchId}`

    socket.join(roomId)
    socket.join(`tournament_${tournamentId}`)

    if (!matchLobby[roomId]) {
      matchLobby[roomId] = { players: new Set(), readyPlayers: new Set(), timerStartedAt: null, timer: null }
    }
    const lobby = matchLobby[roomId]

    if (lobby.players.has(playerName)) {
      socket.emit('match_lobby_update', { matchId, players: [...lobby.players], timerStartedAt: lobby.timerStartedAt })
      return
    }

    lobby.players.add(playerName)

    if (lobby.players.size === 2) {
      lobby.timerStartedAt = Date.now()
      lobby.timer = setTimeout(() => {
        const l = matchLobby[roomId]
        if (!l) return
        const winner = l.readyPlayers.size > 0 ? [...l.readyPlayers][0] : [...l.players][0]
        handleAutoWin(tournamentId, matchId, winner)
      }, MATCH_TIMEOUT_MS)
    }

    io.to(`tournament_${tournamentId}`).emit('match_lobby_update', {
      matchId,
      players: [...lobby.players],
      timerStartedAt: lobby.timerStartedAt,
    })
  })

  // Player clicked "Join Match" — once both click, confirm the match
  socket.on('click_join_match', ({ tournamentId, matchId, playerName }) => {
    if (!tournamentId || !matchId || !playerName) return
    const roomId = `t${tournamentId}_m${matchId}`
    const lobby = matchLobby[roomId]
    if (!lobby) return

    lobby.readyPlayers.add(playerName)

    if (lobby.readyPlayers.size >= 2) {
      if (lobby.timer) { clearTimeout(lobby.timer); lobby.timer = null }
      delete matchLobby[roomId]
      const tournament = tournaments.find((t) => String(t.id) === String(tournamentId))
      const gameHosted = tournament ? tournament.name : String(tournamentId)
      io.to(`tournament_${tournamentId}`).emit('match_confirmed', { matchId, gameHosted })
    }
  })

  // Game-page socket room
  socket.on('join_match', ({ roomId, playerName }) => {
    const name = playerName || socket.request.session?.name
    if (!name || !roomId) return
    socket.join(roomId)
    socket.emit('match_state', { joined: true, name })
  })

  // Save match result and broadcast updated bracket
  socket.on('report_match_result', ({ tournamentId, matchId, winner }) => {
    if (!tournamentId || !matchId || !winner) return
    if (!matchWinners[tournamentId]) matchWinners[tournamentId] = {}
    if (matchWinners[tournamentId][matchId]) return // already recorded
    matchWinners[tournamentId][matchId] = winner
    broadcastBracket(tournamentId)
  })

  // Generic relay — forward game events to the other player in the same room
  ;['ah_state', 'ah_paddle', 'ah_gameover', 'hb_score', 'hb_gameover', 'da_bid', 'da_gameover'].forEach(event => {
    socket.on(event, ({ room, ...data }) => {
      if (!room) return
      socket.to(room).emit(event, data)
    })
  })

  socket.on('disconnect', () => {
    // Players remain in lobby on disconnect (their slot is still assigned).
    // Timer continues so auto-win fires if they don't come back.
  })
})

httpServer.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`)
})
