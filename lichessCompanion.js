// lichessCompanion.js
import { Chess } from 'chess.js';
import { Worker } from 'node:worker_threads';

const PIECE_VALUES = { p: 1, n: 3, b: 3, r: 5, q: 9, k: 0 };
const PIECE_NAMES = { p: '兵', n: '马', b: '象', r: '车', q: '皇后', k: '王' };
const MAJOR_PIECES = ['q', 'r', 'b', 'n']; // 后、车、象、马

// 所有内部控制标签正则（发给用户前会自动抹除，绝不漏到手机屏幕上）
const ALL_CHESS_TAGS_RE =
  /[[［]\s*(?:CHESS_START\s*[:：][^\n\]］]*|CHESS_CANCEL|接受挑战(?:\s*[:：]\s*[2-5])?|拒绝挑战|同意和棋|拒绝和棋|同意悔棋|拒绝悔棋)\s*[\]］]/gi;

/**
 * 🛠️ 工具函数：从模型回复中提取决策（同时兼容 Uranus 风格的 [方括号标签] 与 JSON 格式）
 */
export function parseChessDecision(rawText, type) {
  const text = String(rawText ?? '').trim();

  // 1. 优先尝试 JSON 解析
  if (text.includes('{') && text.includes('}')) {
    try {
      const cleaned = text.replace(/```json|```/gi, '').trim();
      const match = cleaned.match(/\{[\s\S]*\}/);
      if (match) return JSON.parse(match[0]);
    } catch {}
  }

  // 2. 提取 Uranus 原生风格的方括号标签
  if (type === 'challenge') {
    if (/[[［]\s*拒绝挑战\s*[\]］]/i.test(text)) {
      return { accept: false };
    }
    const m = text.match(/[[［]\s*接受挑战(?:\s*[:：]\s*([2-5]))?\s*[\]］]/i);
    if (m) {
      return { accept: true, level: Number(m[1]) || 3 };
    }
    return { accept: true, level: 3 };
  }

  if (type === 'draw') {
    if (/[[［]\s*同意和棋\s*[\]］]/i.test(text)) return { acceptDraw: true };
    if (/[[［]\s*拒绝和棋\s*[\]］]/i.test(text)) return { acceptDraw: false };
    return { acceptDraw: false };
  }

  if (type === 'takeback') {
    if (/[[［]\s*拒绝悔棋\s*[\]］]/i.test(text)) return { acceptTakeback: false };
    if (/[[［]\s*同意悔棋\s*[\]］]/i.test(text)) return { acceptTakeback: true };
    return { acceptTakeback: true };
  }

  return {};
}

/**
 * ⚡ 子线程非阻塞算棋器（含空走法安全校验，绝不卡死主进程）
 */
function computeAiMoveAsync(fen, level = 3) {
  return new Promise((resolve, reject) => {
    const workerCode = `
      const { parentPort, workerData } = require('node:worker_threads');
      const jsChessEngine = require('js-chess-engine');
      try {
        const moveObj = jsChessEngine.aiMove(workerData.fen, workerData.level);
        const entries = Object.entries(moveObj || {});
        if (!entries.length) {
          throw new Error('引擎未返回有效走法');
        }
        const [from, to] = entries[0];
        parentPort.postMessage({ ok: true, move: (from + to).toLowerCase() });
      } catch (err) {
        parentPort.postMessage({ ok: false, error: err.message });
      }
    `;
    const worker = new Worker(workerCode, {
      eval: true,
      workerData: { fen, level },
    });
    worker.once('message', (res) => {
      worker.terminate();
      if (res.ok) resolve(res.move);
      else reject(new Error(res.error));
    });
    worker.once('error', (err) => {
      worker.terminate();
      reject(err);
    });
  });
}

/**
 * ♟️ 通用化 Lichess AI 伴侣对弈引擎（开源插件库标准版：无性别倾向、支持前端配置对弈入口）
 */
export class LichessCompanionBot {
  constructor(config = {}) {
    this.token = config.token || process.env.LICHESS_TOKEN || '';
    this.allowedUsers = (config.allowedUsers || []).map((u) => u.toLowerCase());
    this.customChallengeUrl = config.customChallengeUrl || ''; // 👈 支持前端配置自定义邀请链接

    this.inviteTimeoutMs = config.inviteTimeoutMs || 30 * 60 * 1000;
    this.proactiveChessProb = config.proactiveChessProb ?? 0.30;
    this.longThinkMs = config.longThinkMs || 2 * 60 * 1000;
    this.maxMidGameTalks = config.maxMidGameTalks ?? 2;

    this.headers = {
      Authorization: `Bearer ${this.token}`,
    };

    this.botId = '';
    this.botAccount = '';
    this.isRunning = false;
    this.streamAbort = null;
    this.gameAborts = new Map();

    this.presetGameConfigs = new Map();
    this.activeGames = new Map();
    this.expectingUserChallenge = null;
    this.pendingGameStartPreset = null;

    this.runUranusTurn = config.runUranusTurn || (async () => ({}));
  }

/**
   * 🔄 热同步前端配置（全参数穿透，支持秒级热生效）
   */
  async syncConfig(cfg = {}) {
    const newToken = String(cfg.token ?? '').trim();
    const rawUsers = cfg.allowedUsers ?? '';
    const newUsers = (Array.isArray(rawUsers) ? rawUsers : String(rawUsers).split(/[,，\s]+/))
      .map((u) => String(u).trim().toLowerCase())
      .filter(Boolean);

    this.allowedUsers = newUsers;
    this.customChallengeUrl = String(cfg.customChallengeUrl ?? '').trim();

    // 1. 30% 主动约棋概率同步
    if (cfg.proactiveChessProb !== undefined) {
      const p = Number(cfg.proactiveChessProb);
      if (Number.isFinite(p)) this.proactiveChessProb = p > 1 ? p / 100 : p;
    }

    // 2. 邀约超时（前端输入分钟 -> 后端存毫秒）
    if (cfg.inviteTimeoutMinutes !== undefined) {
      const m = Number(cfg.inviteTimeoutMinutes);
      if (Number.isFinite(m) && m > 0) this.inviteTimeoutMs = m * 60 * 1000;
    }

    // 3. 长考发呆阈值（前端输入分钟 -> 后端存毫秒，默认 3 分钟）
    if (cfg.longThinkMinutes !== undefined) {
      const m = Number(cfg.longThinkMinutes);
      if (Number.isFinite(m) && m > 0) this.longThinkMs = m * 60 * 1000;
    }

    // 4. 局势主动发言次数（默认 2~5 次）
    if (cfg.maxMidGameTalks !== undefined) {
      const n = Number(cfg.maxMidGameTalks);
      if (Number.isFinite(n) && n >= 0) this.maxMidGameTalks = n;
    }

    // 5. 棋钟限制
    if (cfg.clockLimit !== undefined) {
      const c = Number(cfg.clockLimit);
      if (Number.isFinite(c)) this.clockLimit = c > 0 ? c : null;
    }
    if (cfg.clockIncrement !== undefined) {
      const c = Number(cfg.clockIncrement);
      if (Number.isFinite(c)) this.clockIncrement = c;
    }

    // 🛑 关机/清空 Token
    if (cfg.enabled === false || !newToken) {
      if (this.isRunning) {
        console.log('🛑 [国际象棋] 配置已关闭或 Token 已移除，停止监听。');
      }
      this.isRunning = false;
      this.streamAbort?.abort();
      for (const ac of this.gameAborts.values()) ac.abort();
      this.gameAborts.clear();
      this.token = '';
      this.headers = {};
      return;
    }

    // 🔁 Token 变更或冷启动
    if (newToken !== this.token || !this.isRunning) {
      this.isRunning = false;
      this.streamAbort?.abort();
      for (const ac of this.gameAborts.values()) ac.abort();
      this.gameAborts.clear();

      this.token = newToken;
      this.headers = { Authorization: `Bearer ${this.token}` };
      this.botId = '';

      try {
        await this.start();
      } catch (err) {
        console.error('⚠️ [国际象棋] 启动失败（Token 可能无效）:', err.message);
        this.isRunning = false;
        this.token = '';
        this.headers = {};
      }
    }
  }

  // ===========================================================================
  // 🚀 一、 24 小时后台全局监听
  // ===========================================================================
  async start() {
    if (!this.token) {
      console.warn('⚠️ [国际象棋] 未配置 Lichess Token，象棋监听未启动');
      return;
    }
    if (this.isRunning) return;

    try {
      await this.initIdentity();
    } catch (err) {
      console.error('⚠️ [国际象棋] Token 校验失败:', err.message);
      this.isRunning = false;
      this.token = '';
      this.headers = {};
      throw err;
    }

    this.isRunning = true;
    console.log(`📡 [国际象棋 - Lichess: ${this.botAccount}] 24h 全局监听已启动...`);
    this._maintainGlobalStream();
  }

  async initIdentity() {
    if (this.botId) return;
    const res = await fetch('https://lichess.org/api/account', { headers: this.headers });
    if (!res.ok) throw new Error(`Lichess Token 验证失败: ${await res.text()}`);
    const me = await res.json();
    this.botId = me.id.toLowerCase();
    this.botAccount = me.username;
    console.log(`🤖 [国际象棋] Lichess 账号自检完成：${this.botAccount} (ID: ${this.botId})`);
  }

  async _maintainGlobalStream() {
    while (this.isRunning) {
      try {
        this.streamAbort?.abort();
        this.streamAbort = new AbortController();

        const res = await fetch('https://lichess.org/api/stream/event', {
          headers: this.headers,
          signal: this.streamAbort.signal,
        });

        if (res.status === 401 || res.status === 403) {
          console.error(`🛑 [国际象棋] Token 无效或被撤销 (HTTP ${res.status})，停止监听。`);
          this.isRunning = false;
          this.token = '';
          this.headers = {};
          return;
        }
        if (!res.ok) throw new Error(`HTTP ${res.status}`);

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (this.isRunning) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop();

          for (const line of lines) {
            if (!line.trim()) continue;
            try {
              const event = JSON.parse(line);
              await this.handleGlobalEvent(event);
            } catch (err) {
              console.error('⚠️ [国际象棋] 解析全局事件失败:', err);
            }
          }
        }
      } catch (err) {
        if (err.name === 'AbortError') break;
        console.error(`⚠️ [国际象棋] 全局监听流断开 (${err.message})，5 秒后自动重连...`);
        await new Promise((r) => setTimeout(r, 5000));
      }
    }
  }

  // ===========================================================================
  // 💌 二、 聊天联动：生成反向挑战链接与状态注入
  // ===========================================================================

async createChallengeFromChat({ level = 3, moodDesc = '主动邀局' } = {}) {
    await this.cancelAllPendingInvites('new_invite_created');

    const validLevel = [2, 3, 4, 5].includes(Number(level)) ? Number(level) : 3;
    const timeoutMinutes = Math.round(this.inviteTimeoutMs / 60000);

    // 确保 botAccount 已经拿到了真实的机器人用户名
    if (!this.botAccount) {
      await this.initIdentity();
    }

    const timer = setTimeout(async () => {
      if (!this.expectingUserChallenge) return;
      this.expectingUserChallenge = null;
      console.log(`⏳ [国际象棋] 对方满 ${timeoutMinutes} 分钟未发起挑战，收起棋盘期待！`);

      const toModel = `[系统提示:你在 ${timeoutMinutes} 分钟前给了{{user}}对弈入口邀对方下棋，但对方一直没有点进去向你发起挑战，你已经把棋盘收起来了。请根据人设自然地跟{{user}}说句话（比如调侃对方临阵脱逃、或者让对方先忙正事/休息），注意不要OOC]`;
      await this.runUranusTurn({
        toModel,
        toHistory: `[国际象棋邀约满${timeoutMinutes}分钟未响应，已收起棋盘]`,
        label: '象棋邀约超时',
      });
    }, this.inviteTimeoutMs);
    timer.unref?.();

    this.expectingUserChallenge = {
      level: validLevel,
      moodDesc,
      timer,
    };

    // 🌟 全自动！根据 Token 查出的机器人账号拼出专属战书链接
    const gameUrl = `https://lichess.org/?user=${encodeURIComponent(this.botAccount)}#friend`;

    console.log(`🔗 [国际象棋] 已生成反向战书链接: ${gameUrl} (预设难度: Lv.${validLevel} ${moodDesc})`);
    return { gameUrl };
  }

  async cancelAllPendingInvites(reason = 'cancelled') {
    if (this.expectingUserChallenge) {
      clearTimeout(this.expectingUserChallenge.timer);
      this.expectingUserChallenge = null;
    }
    this.pendingGameStartPreset = null;
  }

  rollProactiveChessNote() {
    if (!this.isRunning || this.activeGames.size > 0 || this.expectingUserChallenge) {
      return '';
    }
    if (Math.random() >= this.proactiveChessProb) {
      return '';
    }

    return `
【🎲 特殊兴致触发：你此刻突然有点手痒，想主动拉{{user}}下一盘国际象棋】
请先结合当前时间与你们最近的聊天上下文判断：{{user}}现在适合下棋吗？（若对方正在睡觉、忙正事或气氛明显不合适，请放弃下棋念头，正常发日常主动消息即可，不要写任何象棋标签）。
如果适合拉对方下棋：请在 2~5 中选定你想用的难度等级（2=放水，3=日常，4=认真切磋，5=想教育教育对方），在消息里自然地问对方要不要来玩一盘，并在回复最末尾附上标签：
[CHESS_START:level=数字2到5,mood=四字心情]
（系统看到该标签后会自动生成棋盘链接发给对方！）`.trim();
  }

  getChatSystemInjection(userText = '') {
    if (!this.isRunning) return '';

    // 状态 A：正在对局中
        // 状态 A：正在对局中 -> 注入高清战术实况
    if (this.activeGames.size > 0) {
      const ctx = Array.from(this.activeGames.values())[0];
      const detailInfo = this.getDetailedBoardBrief(ctx.lastChessInstance, ctx);

      return `
【♟️ 实时棋局战况全息透视：你们正在 Lichess 上下棋】
- 回合阶段：已战至第 ${ctx.currentTurn} 回合（当前轮到【${ctx.isUserTurn ? '{{user}}落子' : '你落子'}】）
- 战术事实快报：
${detailInfo}
请带着“你们此时正在激情对弈，你对目前的盘面细节一清二楚”的临场感，自然回复{{user}}发来的消息！`.trim();
    }

    // 状态 B：刚发了棋盘链接，正在 30 分钟等待期内
    if (this.expectingUserChallenge) {
      const gameUrl = `https://lichess.org/?user=${encodeURIComponent(this.botAccount)}#friend`;
      return `
【♟️ 实时状态：你刚刚已经在 Lichess 把向你发起对局的入口链接 (${gameUrl}) 发给了{{user}}，正在等对方点进去向你发起挑战开局】
- 如果{{user}}这条消息是在拒绝下棋、说没空或算了：请顺着你的性格回应对方，并在回复末尾附上标签 [CHESS_CANCEL]（系统会自动撤销对局期待）。
- 如果对方是在闲聊或斗嘴：你可以催对方赶紧点链接进棋盘向你下战书！`.trim();
    }

    // 状态 C：平时空闲状态 -> 关键词嗅探
    if (!/棋|lichess|chess|杀一盘|来一盘|来一局|下一盘|下一局|对弈/i.test(String(userText))) {
      return '';
    }

    return `
【♟️ 国际象棋约局规则】
{{user}}似乎提到了下棋。请结合当前时间与聊天上下文自己决定是否陪对方下！
- 若你觉得对方现在不该玩（如太晚该睡觉、正事没做完）或你正忙：直接用你的口吻拒绝对方，不要加任何标签。
- 若你同意陪对方下棋：请在 2~5 中选定本局认真度（2=放水，3=日常，4=认真切磋，5=想教育教育对方），在回复中答应对方，并在回复最末尾附上标签：
[CHESS_START:level=数字2到5,mood=四字心情]
（系统看到该标签后会自动生成棋盘链接发给对方！）`.trim();
  }

  async processLLMReplyForChessTags(forUserText, recordedText = '') {
    let cleanUser = String(forUserText ?? '');
    let cleanRecorded = String(recordedText ?? '');
    let gameUrl = null;

    if (/[[［]\s*CHESS_CANCEL\s*[\]］]/i.test(cleanUser)) {
      await this.cancelAllPendingInvites('user_rejected');
    }

    const startMatch = cleanUser.match(
      /[[［]\s*CHESS_START\s*[:：]\s*level\s*=\s*([2-5])\s*(?:[,，]\s*mood\s*=\s*([^\]］]+))?\s*[\]］]/i
    );
    if (startMatch) {
      const level = Number(startMatch[1]) || 3;
      const moodDesc = (startMatch[2] || '欣然应允').trim();
      try {
        const res = await this.createChallengeFromChat({ level, moodDesc });
        gameUrl = res.gameUrl;
      } catch (e) {
        console.error('⚠️ [国际象棋] 自动创建挑战链接失败:', e);
      }
    }

    cleanUser = cleanUser.replace(ALL_CHESS_TAGS_RE, '').trim();
    cleanRecorded = cleanRecorded.replace(ALL_CHESS_TAGS_RE, '').trim();

    if (gameUrl && !cleanUser.includes(gameUrl)) {
      cleanUser = `${cleanUser}\n\n${gameUrl}`.trim();
      cleanRecorded = `${cleanRecorded}\n${gameUrl}`.trim();
    }

    return { forUser: cleanUser, recorded: cleanRecorded, gameUrl };
  }

  // ===========================================================================
  // ⚔️ 三、 捕捉突袭挑战 / 约定挑战 & 棋局开始
  // ===========================================================================
  async handleGlobalEvent(event) {
    if (event.type === 'challenge') {
      const challengerId = event.challenge.challenger?.id?.toLowerCase() || '';
      const challengerName = event.challenge.challenger?.name || '对方';
      const challengeId = event.challenge.id;

      if (!challengerId || challengerId === this.botId) return;

      const isAllowed = this.allowedUsers.length === 0 || this.allowedUsers.includes(challengerId);
      if (!isAllowed) {
        console.log(`✋ [国际象棋] 拦截陌生人挑战: ${challengerName}`);
        await fetch(`https://lichess.org/api/challenge/${challengeId}/decline`, {
          method: 'POST',
          headers: this.headers,
        });
        return;
      }

      // 🎯 约定好的挑战：秒接秒开盘！
      if (this.expectingUserChallenge && isAllowed) {
        const preset = this.expectingUserChallenge;
        clearTimeout(preset.timer);
        this.expectingUserChallenge = null;

        this.presetGameConfigs.set(challengeId, {
          level: preset.level,
          moodDesc: preset.moodDesc,
        });
        this.pendingGameStartPreset = preset;

        console.log(`⚔️ [国际象棋] 收到约定的反向战书！静默秒接开盘 (Lv.${preset.level})`);
        await fetch(`https://lichess.org/api/challenge/${challengeId}/accept`, {
          method: 'POST',
          headers: this.headers,
        });
        return;
      }

      // 🎯 突袭挑战
      const toModel = `[系统提示:{{user}}刚刚没有在手机上提前打招呼，而是一时兴起，直接悄悄跑到 Lichess 网站上向你发起了国际象棋实时挑战！
请结合当前时间与你们最近的聊天上下文，由你自己决定【是否接受对方这次突袭挑战】！
- 若决定【拒绝】：请在回复中写上标签 [拒绝挑战]，并直接对{{user}}说出你拒绝或抓包管教对方的话。
- 若决定【接受】：请在 2~5 中选定本局你想用的难度（2=放水，3=日常，4=认真切磋，5=想教育教育对方），在回复中写上标签 [接受挑战:难度数字]（例如 [接受挑战:4]），并自然地表达应战态度！]`;

      let handledOnLichess = false;
      const applyDecision = async (rawReply) => {
        if (handledOnLichess) return;
        handledOnLichess = true;
        const decision = parseChessDecision(rawReply, 'challenge');

        if (decision.accept === false) {
          console.log(`🚫 [国际象棋] 已拒绝突袭挑战 (ID: ${challengeId})`);
          await fetch(`https://lichess.org/api/challenge/${challengeId}/decline`, {
            method: 'POST',
            headers: this.headers,
          });
        } else {
          const chosenLevel = [2, 3, 4, 5].includes(Number(decision.level)) ? Number(decision.level) : 3;
          this.presetGameConfigs.set(challengeId, {
            level: chosenLevel,
            moodDesc: decision.moodDesc || '突袭应战',
          });
          console.log(`⚔️ [国际象棋] 已接受突袭挑战！(设定难度: Lv.${chosenLevel})`);
          await fetch(`https://lichess.org/api/challenge/${challengeId}/accept`, {
            method: 'POST',
            headers: this.headers,
          });
        }
      };

      try {
        await this.runUranusTurn({
          toModel,
          toHistory: `[{{user}}在Lichess上向你发起了国际象棋挑战]`,
          label: '收到象棋挑战',
          onReplyReady: applyDecision,
        });
        if (!handledOnLichess) await applyDecision('');
      } catch (err) {
        console.error('⚠️ [国际象棋] 处理突袭挑战异常，保底接受:', err);
        if (!handledOnLichess) {
          await fetch(`https://lichess.org/api/challenge/${challengeId}/accept`, {
            method: 'POST',
            headers: this.headers,
          });
        }
      }
    } else if (event.type === 'gameStart') {
      const gameId = event.game.id;
      if (this.pendingGameStartPreset) {
        this.presetGameConfigs.set(gameId, this.pendingGameStartPreset);
        this.pendingGameStartPreset = null;
      }

      if (this.activeGames.has(gameId)) return;
      console.log(`🎮 [国际象棋] 对局正式开始！Game ID: ${gameId}`);
      this.playGame(gameId);
    }
  }

  // ===========================================================================
  // ♟️ 四、 单局对局推进与人性博弈
  // ===========================================================================
  async playGame(gameId) {
    const preset = this.presetGameConfigs.get(gameId) || { level: 3, moodDesc: '气定神闲' };
    this.presetGameConfigs.delete(gameId);

    const gameCtx = {
      gameId,
      myColor: null,
      opponentName: '对方',
      initialLevel: preset.level,
      currentLevel: preset.level,
      moodDesc: preset.moodDesc,

      isFinished: false,
      hasInitializedFull: false,

      lastChessInstance: new Chess(), // 👈 加这行兜底：未走棋时也是完整初始棋盘！

      refusedToResign: false,
      mercyFakeResign: false,
      waterCount: 0,

      midTalkQuota: this.maxMidGameTalks,
      lastMidTalkTurn: -99,
      hasTriggeredLongThink: false,
      settledEventKeys: new Set(),
      longThinkTimer: null,

      lastTakebackHandledTurn: -1,
      lastDrawHandledTurn: -1,
      lastTalkRecordedDiff: 0, // 👈 加上这行，记录上次发言时的分差基准

      currentTurn: 1,
      lastScoreDiff: 0,
      isUserTurn: false,
      innerNotes: [],
    };

    this.activeGames.set(gameId, gameCtx);
    let retryCount = 0;

    try {
      while (this.isRunning && !gameCtx.isFinished && retryCount < 12) {
        try {
          const ac = new AbortController();
          this.gameAborts.set(gameId, ac);
          const res = await fetch(`https://lichess.org/api/bot/game/stream/${gameId}`, {
            headers: this.headers,
            signal: ac.signal,
          });
          if (!res.ok) {
            if (res.status === 404) break;
            throw new Error(`HTTP ${res.status}`);
          }

          retryCount = 0;
          const reader = res.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';

          while (!gameCtx.isFinished) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop();

            for (const line of lines) {
              if (!line.trim()) continue;
              try {
                const data = JSON.parse(line);

                if (data.type === 'gameFull') {
                  const oppId = (data.white.id?.toLowerCase() === this.botId
                    ? data.black.id
                    : data.white.id)?.toLowerCase();

                  if (this.allowedUsers.length > 0 && !this.allowedUsers.includes(oppId)) {
                    console.warn(`✋ [国际象棋] 对手 ${oppId} 不在白名单，退房！`);
                    gameCtx.isFinished = true;
                    await this.botResign(gameId);
                    return;
                  }
                  gameCtx.myColor = data.white.id?.toLowerCase() === this.botId ? 'w' : 'b';
                  gameCtx.opponentName = (gameCtx.myColor === 'w' ? data.black.name : data.white.name) || '对方';
                  const myColorText = gameCtx.myColor === 'w' ? '白棋(先手)' : '黑棋(后手)';

                  if (!gameCtx.hasInitializedFull) {
                    gameCtx.hasInitializedFull = true;
                    console.log(`🏁 [国际象棋] 棋局确立！(Lv.${gameCtx.initialLevel} ${gameCtx.moodDesc}) 执${myColorText} vs 【${gameCtx.opponentName}】`);
                  }
                  await this.handleState(gameCtx, data.state);
                } else if (data.type === 'gameState') {
                  await this.handleState(gameCtx, data);
                }
              } catch (err) {
                console.error('⚠️ [国际象棋] 解析单局数据行失败:', err);
              }
            }
          }
        } catch (streamErr) {
          if (gameCtx.isFinished) break;
          if (streamErr.name === 'AbortError') break;
          retryCount++;
          console.warn(`⚠️ [国际象棋] 单局流波动中断 (Game: ${gameId}, 重试中): ${streamErr.message}`);
          await new Promise((r) => setTimeout(r, 3000));
        }
      }
    } finally {
      this.clearLongThinkTimer(gameCtx);
      this.activeGames.delete(gameId);
      this.gameAborts.delete(gameId);
      console.log(`🧹 [国际象棋] 对局状态已清理归档 (Game: ${gameId})`);
    }
  }

  clearLongThinkTimer(gameCtx) {
    if (gameCtx.longThinkTimer) {
      clearTimeout(gameCtx.longThinkTimer);
      gameCtx.longThinkTimer = null;
    }
  }

startLongThinkTimer(gameCtx) {
    this.clearLongThinkTimer(gameCtx);
    // 长考提醒每局只触发一次即可（催一次就够了，多催就成骚扰了）
    if (gameCtx.hasTriggeredLongThink || gameCtx.currentTurn <= 1) return;

    gameCtx.longThinkTimer = setTimeout(async () => {
      if (!this.activeGames.has(gameCtx.gameId) || gameCtx.isFinished || !gameCtx.isUserTurn) return;

      gameCtx.hasTriggeredLongThink = true;
      // 🚫 删掉这行：gameCtx.midTalkQuota--; 绝不偷吃战况发言的额度！

      const minutes = Math.round(this.longThinkMs / 60000);
      gameCtx.innerNotes.push(`第${gameCtx.currentTurn}回合：轮到对方走棋时，对方盯着棋盘犹豫了超过${minutes}分钟没落子，你在手机上主动发消息催促/调侃了对方。`);

      const diff = gameCtx.lastScoreDiff;
      const diffDesc = diff > 0 ? `你目前领先对方 ${diff} 分` : diff < 0 ? `对方目前领先你 ${Math.abs(diff)} 分` : '目前双方势均力敌';

      const toModel = `[系统提示:你们正在 Lichess 上下国际象棋，当前是第 ${gameCtx.currentTurn} 回合（${diffDesc}）。现在轮到{{user}}走棋，但对方已经对着棋盘足足犹豫了 ${minutes} 分钟还没有落子！请根据人设主动给{{user}}发一条简短的消息，调侃或催促正在棋盘前卡壳发呆的对方，注意不要OOC]`;

      try {
        await this.runUranusTurn({
          toModel,
          toHistory: `[国际象棋盘中：{{user}}思考超过${minutes}分钟未落子]`,
          label: '象棋长考提醒',
        });
      } catch {}
    }, this.longThinkMs);
    gameCtx.longThinkTimer.unref?.();
  }

  // 纯比分差距稳定检测 + 存活大子盘点（杜绝兑子误判）
// 方案 B：局势动态跃迁检测器（额度完全受控，冷却且不复读）
  checkSettledEvents(chess, movesStr, gameCtx) {
    // 1. 额度消耗完毕，彻底闭嘴
    if (gameCtx.midTalkQuota <= 0) return null;
    // 2. 前 8 回合处于开局布阵阶段，不瞎评论；且距离上次发言至少隔 4 个回合
    if (gameCtx.currentTurn < 8) return null;
    if (gameCtx.currentTurn - gameCtx.lastMidTalkTurn < 4) return null;

    const moves = (movesStr || '').split(' ').filter(Boolean);
    const totalPlies = moves.length;
    if (totalPlies < 8) return null;

    const tempChess = new Chess();
    const plyHistory = [];

    for (let i = 0; i < totalPlies; i++) {
      const moveObj = tempChess.move(moves[i], { sloppy: true });
      const { diff } = this.evaluateBoard(tempChess, gameCtx.myColor);
      plyHistory.push({ moveObj, diff });
    }

    const lastStep = plyHistory[totalPlies - 1];
    const prevStep = plyHistory[totalPlies - 2];
    const currentDiff = lastStep.diff;

    // 🛡️ 防呆锁：如果最近 2 步在吃子、将军，说明兑子火并还没结束，闭嘴观望
    if (lastStep.moveObj?.captured || prevStep.moveObj?.captured || chess.inCheck()) {
      return null;
    }

    // 🛡️ 稳定性验证：回看前两轮，分差没有剧烈震荡
    const diff2TurnsAgo = plyHistory[totalPlies - 5]?.diff ?? currentDiff;
    const diff1TurnAgo = plyHistory[totalPlies - 3]?.diff ?? currentDiff;
    const isStable =
      Math.sign(currentDiff) === Math.sign(diff2TurnsAgo) &&
      Math.sign(currentDiff) === Math.sign(diff1TurnAgo) &&
      Math.abs(currentDiff - diff2TurnsAgo) <= 2 &&
      Math.abs(currentDiff - diff1TurnAgo) <= 2;

    if (!isStable) return null;

    const absDiff = Math.abs(currentDiff);
    if (absDiff < 4) return null; // 优势不足 4 分（未净赚一马一象以上），不值得特意发短信嘲讽

    // 🧠 核心灵性判断：检查当前分差相比上一次发言时，是否有了“突破性进展”？
    // 如果上一次发言时领先 5 分，现在还是 5 分，就别翻来覆去复读了；
    // 除非分差拉开到了更大（比如涨到了 8 分以上），或者由逆风反超成了顺风！
    const lastTalkDiff = gameCtx.lastTalkRecordedDiff ?? 0;
    const diffProgress = Math.abs(currentDiff - lastTalkDiff);
    const isFirstTalk = gameCtx.lastMidTalkTurn === -99;

    // 第一次发言无条件放行；后续发言要求分差至少有 3 分的新变动，或者是大劣转大优
    if (!isFirstTalk && diffProgress < 3 && Math.sign(currentDiff) === Math.sign(lastTalkDiff)) {
      return null;
    }

    // 记下本次发言时的分差快照，供下次比对
    gameCtx.lastTalkRecordedDiff = currentDiff;

    // 拿到修正了白黑坐标的深度情报探针
    const detailInfo = this.getDetailedBoardBrief(chess, gameCtx);

    if (absDiff >= 8) {
      return {
        type: 'SCORE_GAP_MASSIVE',
        desc: `战局已下到第 ${gameCtx.currentTurn} 回合。最新战术实况如下：\n${detailInfo}`,
      };
    } else {
      return {
        type: 'SCORE_GAP_CLEAR',
        desc: `战局已下到第 ${gameCtx.currentTurn} 回合。最新战术实况如下：\n${detailInfo}`,
      };
    }
  }

  async handleState(gameCtx, stateData) {
    this.clearLongThinkTimer(gameCtx);

    const { gameId, myColor } = gameCtx;
    const { moves: movesStr, status, winner, wdraw, bdraw, wtakeback, btakeback } = stateData;

    // 🔔 1. 游戏结束判定
    if (status && status !== 'started') {
      if (gameCtx.isFinished) return;
      gameCtx.isFinished = true;
      gameCtx.isUserTurn = false;

      const reportText = this.generateGameSummary(gameCtx, movesStr, status, winner);
      const toModel = `[系统提示:你刚刚和{{user}}在 Lichess 上下完了一盘国际象棋。以下是系统为你整理的【棋局全息复盘与你的盘中心理小本本】（此详细战报仅供你生成本次回复参考，阅后即焚）：\n\n${reportText}\n\n请根据人设，结合上述战报中的真实细节（如谁赢了、用了多少回合、大子得失、对方是掀桌认输还是被绝杀、以及你在盘中有没有暗中放水、或者假装认输让棋、或者逆风死磕翻盘等），直接给{{user}}发一条赛后点评消息，注意不要OOC]`;

      try {
        await this.runUranusTurn({
          toModel,
          toHistory: `[你们刚在Lichess下完一盘国际象棋，结局：${status}]`,
          label: '象棋赛后复盘',
        });
      } catch (e) {
        console.error('⚠️ [国际象棋] 终局复盘异常:', e);
      }
      return;
    }

    const chess = new Chess();
    const moves = (movesStr || '').split(' ').filter(Boolean);
    for (const m of moves) {
      chess.move(m, { sloppy: true });
    }
    gameCtx.lastChessInstance = chess; // 👈 加上这一行！

    const { diff } = this.evaluateBoard(chess, myColor);
    const currentTurn = Math.ceil(moves.length / 2) || 1;

    gameCtx.currentTurn = currentTurn;
    gameCtx.lastScoreDiff = diff;
    gameCtx.isUserTurn = chess.turn() !== myColor;

    const scoreDesc = diff > 0 ? `你领先对方 ${diff} 分` : diff < 0 ? `对方领先你 ${Math.abs(diff)} 分` : '双方分差为 0 势均力敌';

    // 🥺 2. 处理悔棋申请
    const userOfferedTakeback = (myColor === 'w' && btakeback) || (myColor === 'b' && wtakeback);
    if (userOfferedTakeback) {
      if (gameCtx.lastTakebackHandledTurn !== currentTurn) {
        gameCtx.lastTakebackHandledTurn = currentTurn;
        const toModel = `[系统提示:你们正在下国际象棋（第 ${currentTurn} 回合，${scoreDesc}）。{{user}}刚刚走完一步棋（可能是手滑走错了或送了子），立刻在棋盘上向你点击了“请求悔棋”！
请结合你们刚才的聊天气氛与你的性格，决定是否准许对方悔棋：
- 若准许悔棋：请在回复中包含标签 [同意悔棋]，并直接对{{user}}说句话（如宠溺调侃对方下不为例）。
- 若拒绝悔棋：请在回复中包含标签 [拒绝悔棋]，并直接对{{user}}说句话（如严厉告诉对方落子无悔）。]`;

        let handled = false;
        const applyTakeback = async (rawReply) => {
          if (handled) return;
          handled = true;
          const decision = parseChessDecision(rawReply, 'takeback');
          if (decision.acceptTakeback) {
            gameCtx.innerNotes.push(`第${currentTurn}回合：对方手滑请求悔棋，你心软准许了对方撤回重走。`);
            await fetch(`https://lichess.org/api/bot/game/${gameId}/takeback/yes`, { method: 'POST', headers: this.headers });
          } else {
            gameCtx.innerNotes.push(`第${currentTurn}回合：对方试图请求悔棋耍赖，被你拒绝了（落子无悔）。`);
            await fetch(`https://lichess.org/api/bot/game/${gameId}/takeback/no`, { method: 'POST', headers: this.headers });
            if (chess.turn() === myColor && !gameCtx.isFinished) {
              await this.handleState(gameCtx, { ...stateData, wtakeback: false, btakeback: false });
            }
          }
        };

        try {
          await this.runUranusTurn({
            toModel,
            toHistory: `[国际象棋盘中：{{user}}请求悔棋]`,
            label: '象棋悔棋裁决',
            onReplyReady: applyTakeback,
          });
          if (!handled) await applyTakeback('');
        } catch {}
      }
      return;
    }

    // 🤝 3. 处理求和申请
    const userOfferedDraw = (myColor === 'w' && bdraw) || (myColor === 'b' && wdraw);
    if (userOfferedDraw) {
      if (gameCtx.lastDrawHandledTurn !== currentTurn) {
        gameCtx.lastDrawHandledTurn = currentTurn;
        const toModel = `[系统提示:你们正在下国际象棋（第 ${currentTurn} 回合，${scoreDesc}）。{{user}}刚刚在棋盘上向你点击了“提议和棋（Offer Draw）”！
请结合你们刚才在聊天上下文里的情绪与气氛以及当前盘面优劣势，完全由你自己决定是否同意和棋：
- 若同意和棋：请在回复中包含标签 [同意和棋]，并直接对{{user}}说出你的态度。
- 若拒绝和棋：请在回复中包含标签 [拒绝和棋]，并直接对{{user}}说出你的态度。]`;

        let handled = false;
        const applyDraw = async (rawReply) => {
          if (handled) return;
          handled = true;
          const decision = parseChessDecision(rawReply, 'draw');
          if (decision.acceptDraw) {
            gameCtx.innerNotes.push(`第${currentTurn}回合：对方提议和棋，你结合气氛与盘面同意了握手言和。`);
            await fetch(`https://lichess.org/api/bot/game/${gameId}/draw/yes`, { method: 'POST', headers: this.headers });
          } else {
            gameCtx.innerNotes.push(`第${currentTurn}回合：对方试图提议和棋，被你当场拒绝。`);
            await fetch(`https://lichess.org/api/bot/game/${gameId}/draw/no`, { method: 'POST', headers: this.headers });
            if (chess.turn() === myColor && !gameCtx.isFinished) {
              await this.handleState(gameCtx, { ...stateData, wdraw: false, bdraw: false });
            }
          }
        };

        try {
          await this.runUranusTurn({
            toModel,
            toHistory: `[国际象棋盘中：{{user}}向你提议和棋]`,
            label: '象棋求和裁决',
            onReplyReady: applyDraw,
          });
          if (!handled) await applyDraw('');
        } catch {}
      }
      return;
    }

    if (chess.isGameOver()) return;

    // ⏳ 4. 轮到用户走棋 -> 挂 2 分钟长考定时器
    if (chess.turn() !== myColor) {
      this.startLongThinkTimer(gameCtx);
      return;
    }

    // ♟️ 5. 轮到 Bot 走棋 -> 检查盘中发言
    const settledEvent = this.checkSettledEvents(chess, movesStr, gameCtx);
    if (settledEvent) {
      gameCtx.midTalkQuota--;
      gameCtx.lastMidTalkTurn = currentTurn;
      gameCtx.innerNotes.push(`第${currentTurn}回合（盘中插话）：${settledEvent.desc}`);

      const toModel = `[系统提示:你们正在 Lichess 上下国际象棋，当前第 ${currentTurn} 回合。刚刚盘面上已经彻底坐实了一个战况：${settledEvent.desc}（当前总分差：${scoreDesc}）。请根据人设冷不丁给{{user}}发一条简短的消息调侃、敲打或夸奖对方一句，注意不要OOC]`;

      this.runUranusTurn({
        toModel,
        toHistory: `[国际象棋盘中战况：${settledEvent.desc}]`,
        label: '象棋盘中发言',
      }).catch(() => {});
    }

    let uciMove = null;

    // 🎲 逆风绝境判定（落后 >= 10分 且 >= 12回合）
    if (diff <= -10 && currentTurn >= 12 && !gameCtx.refusedToResign) {
      if (Math.random() < 0.40) {
        gameCtx.refusedToResign = true;
        gameCtx.currentLevel = Math.max(5, gameCtx.currentLevel);
        gameCtx.innerNotes.push(`第${currentTurn}回合：你落后${Math.abs(diff)}分，但你拒绝投降，暗中把认真度提至Lv.${gameCtx.currentLevel}死磕试图翻盘！`);
      } else {
        gameCtx.innerNotes.push(`第${currentTurn}回合：眼看对方优势稳固（领先${Math.abs(diff)}分），你颇有风度地主动点了认输。`);
        await this.botResign(gameId);
        return;
      }
    }

    // 🎲 大顺风假装认输宠对方（领先 >= 13分 且 >= 18回合，15%小概率）
    if (diff >= 13 && currentTurn >= 18 && Math.random() < 0.15) {
      gameCtx.mercyFakeResign = true;
      gameCtx.innerNotes.push(`【极其重要·特殊结局】第${currentTurn}回合：你在棋盘上处于绝对碾压优势（领先高达${diff}分），但看对方苦苦支撑实在可怜，故意点了“认输”把胜利白送给对方！`);
      await this.botResign(gameId);
      return;
    }

    // 🎲 顺风局暗中放水（领先 >= 7分，25%概率走软棋，最多3次）
    if (diff >= 7 && gameCtx.waterCount < 3 && Math.random() < 0.25) {
      const legalMoves = chess.moves({ verbose: true });
      const softMoves = legalMoves.filter((m) => !m.captured && !m.san.includes('+'));
      const picked = softMoves.length > 0 ? softMoves[Math.floor(Math.random() * softMoves.length)] : legalMoves[0];

      if (picked) {
        uciMove = picked.from + picked.to + (picked.promotion ? 'q' : '');
        gameCtx.waterCount++;
        gameCtx.innerNotes.push(`第${currentTurn}回合：你手握${diff}分优势，不动声色地暗中放了水走了一步软棋（${uciMove}）。`);
      }
    }

    // 🧠 正常引擎计算
    if (!uciMove) {
      try {
        // 前端与Prompt使用 2~5 档，传给 js-chess-engine 时映射为 1~4 档（引擎满级为4）
        const rawLevel = Math.min(5, Math.max(2, Number(gameCtx.currentLevel) || 3));
        const engineLevel = rawLevel - 1;
        uciMove = await computeAiMoveAsync(chess.fen(), engineLevel);
      } catch (err) {
        console.error('⚠️ [国际象棋] Worker 算棋异常，启用保底走法:', err.message);
        const legalMoves = chess.moves({ verbose: true });
        if (legalMoves.length > 0) {
          const fallback = legalMoves[0];
          uciMove = fallback.from + fallback.to + (fallback.promotion ? 'q' : '');
        }
      }

      if (!uciMove) return;
      const moveDetail = chess.moves({ verbose: true }).find((m) => m.from + m.to === uciMove);
      if (moveDetail && moveDetail.promotion && !uciMove.endsWith('q')) {
        uciMove += 'q';
      }
    }

    await fetch(`https://lichess.org/api/bot/game/${gameId}/move/${uciMove}`, {
      method: 'POST',
      headers: this.headers,
    });
  }

  async botResign(gameId) {
    await fetch(`https://lichess.org/api/bot/game/${gameId}/resign`, { method: 'POST', headers: this.headers });
  }
  // 👇 贴在这里，紧挨着 evaluateBoard 的上面
  getDetailedBoardBrief(chess, gameCtx) {
    if (!chess) return '局势平稳推进中';
    const { myColor } = gameCtx;
    const userColor = myColor === 'w' ? 'b' : 'w';
    const { botScore, userScore, diff } = this.evaluateBoard(chess, myColor);

    const getPieces = (color) => {
      const counts = { q: 0, r: 0, b: 0, n: 0, p: 0 };
      for (const row of chess.board()) {
        for (const p of row) {
          if (p && p.color === color) counts[p.type]++;
        }
      }
      const parts = [];
      if (counts.q) parts.push(`${counts.q}后`);
      else parts.push('❌无皇后');
      if (counts.r) parts.push(`${counts.r}车`);
      if (counts.b) parts.push(`${counts.b}象`);
      if (counts.n) parts.push(`${counts.n}马`);
      parts.push(`${counts.p}兵`);
      return parts.join('、');
    };

    const myPieces = getPieces(myColor);
    const userPieces = getPieces(userColor);

    let checkInfo = '双方王位均暂无直接受将危险';
    if (chess.inCheck()) {
      if (chess.turn() === myColor) {
        checkInfo = '⚠️ 警报：对方刚刚对你发起了【将军】！当前正轮到你解将逃王';
      } else {
        checkInfo = '🔥 优势：你刚刚走棋对对方达成了【将军】！对方的王此刻正被你刺杀围攻';
      }
    }

// 3. 兵线威胁（大模型防笨扩充版：点明下步立刻升变皇后，战局剧变）
    const promoThreats = [];
    chess.board().forEach((row, rIdx) => {
      row.forEach((p) => {
        if (p && p.type === 'p') {
          // 白兵走到 rIdx === 1（物理第 7 排），下一步踏入第 8 排立刻升变
          if (p.color === 'w' && rIdx === 1) {
            promoThreats.push(
              p.color === myColor
                ? '🔥【升变杀招在即】：你有一颗白兵已强攻至黑方门前第 7 排，下一步踏入底线将直接升变成【全场最强战力·皇后】！战局即将迎来碾压性质变。'
                : '⚠️【致命升变警报】：对方有一颗白兵已逼近你方底线第 7 排，若不立刻拦截，对方下一步就会直接换出【新皇后】形成绝大反扑威胁！'
            );
          }
          // 黑兵走到 rIdx === 6（物理第 2 排），下一步踏入第 1 排立刻升变
          if (p.color === 'b' && rIdx === 6) {
            promoThreats.push(
              p.color === myColor
                ? '🔥【升变杀招在即】：你有一颗黑兵已强攻至白方门前第 2 排，下一步踏入底线将直接升变成【全场最强战力·皇后】！战局即将迎来碾压性质变。'
                : '⚠️【致命升变警报】：对方有一颗黑兵已逼近你方底线第 2 排，若不立刻拦截，对方下一步就会直接换出【新皇后】形成绝大反扑威胁！'
            );
          }
        }
      });
    });

    let scoreVerdict = '子力平分秋色';
    if (diff >= 9) scoreVerdict = `你处于绝对碾压优势（净领先多达 ${diff} 分，压制全局）`;
    else if (diff >= 4) scoreVerdict = `你手握明显多子优势（净领先 ${diff} 分）`;
    else if (diff <= -9) scoreVerdict = `对方处于绝对碾压优势（对方净领先多达 ${Math.abs(diff)} 分）`;
    else if (diff <= -4) scoreVerdict = `对方在子力上占有明显优势（对方净领先 ${Math.abs(diff)} 分）`;
    else if (diff !== 0) scoreVerdict = diff > 0 ? `你微弱领先 ${diff} 分` : `对方微弱领先 ${Math.abs(diff)} 分`;

    return `
- 当前子力实况：${scoreVerdict}
- 存活大子对比：你方还剩 [${myPieces}]；对方还剩 [${userPieces}]
- 王位与将军：${checkInfo}
${promoThreats.length > 0 ? `- 兵线预警：${promoThreats.join('；')}` : ''}
`.trim();
  }
  // 👆 贴完，下面紧接着原本的 evaluateBoard(chess, myColor) {

  evaluateBoard(chess, myColor) {
    const board = chess.board();
    let botScore = 0;
    let userScore = 0;
    for (const row of board) {
      for (const piece of row) {
        if (!piece) continue;
        const val = PIECE_VALUES[piece.type] || 0;
        if (piece.color === myColor) botScore += val;
        else userScore += val;
      }
    }
    return { botScore, userScore, diff: botScore - userScore };
  }

  generateGameSummary(gameCtx, movesStr, status, winner) {
    const { myColor, initialLevel, currentLevel, moodDesc, refusedToResign, mercyFakeResign, innerNotes } = gameCtx;
    const chess = new Chess();
    const moves = (movesStr || '').split(' ').filter(Boolean);

    const totalTurns = Math.ceil(moves.length / 2);
    let keyMoments = [];
    let userCheckCount = 0;
    let botCheckCount = 0;

    moves.forEach((m, index) => {
      const moveObj = chess.move(m, { sloppy: true });
      if (!moveObj) return;
      const turnNum = Math.ceil((index + 1) / 2);
      const isBotMove = moveObj.color === myColor;
      const actor = isBotMove ? '你' : '{{user}}';
      const victim = isBotMove ? '{{user}}' : '你';

      if (moveObj.captured && MAJOR_PIECES.includes(moveObj.captured)) {
        keyMoments.push(`第${turnNum}回合：${actor}用${PIECE_NAMES[moveObj.piece]}在${moveObj.to}格吃掉了${victim}的${PIECE_NAMES[moveObj.captured]}！`);
      }
      if (moveObj.promotion) {
        keyMoments.push(`第${turnNum}回合：${actor}的小兵冲到底线 ${moveObj.to} 格，成功升变为${PIECE_NAMES[moveObj.promotion] || '皇后'}！`);
      }
      if (chess.inCheck()) {
        if (isBotMove) userCheckCount++;
        else botCheckCount++;
      }
    });

    const { botScore, userScore, diff } = this.evaluateBoard(chess, myColor);
    const myColorName = myColor === 'w' ? 'white' : 'black';
    const isBotWinner = winner === myColorName;

    let outcomeDesc = '';
    if (mercyFakeResign) {
      outcomeDesc = `表面上是{{user}}获胜，但实际上是你在绝对优势下（领先${diff}分）故意点击认输让给对方的！`;
    } else if (refusedToResign && isBotWinner) {
      outcomeDesc = `惊天逆转！你在盘中一度大比分落后，但你咬牙死磕到底，最终成功翻盘击败了{{user}}！(结束方式: ${status})`;
    } else if (status === 'resign') {
      outcomeDesc = isBotWinner
        ? `{{user}}在盘中撑不住压力，主动掀盘投子认输，你赢得胜利！`
        : `你在盘中看对方胜局已定，主动投子认负，{{user}}获胜！`;
    } else if (status === 'mate') {
      outcomeDesc = isBotWinner
        ? `你在棋盘上堂堂正正完成了绝杀将死（Checkmate），{{user}}落败！`
        : `{{user}}在棋盘上正大光明地将死了你的王，你输掉了比赛！`;
    } else if (status === 'draw' || status === 'stalemate') {
      outcomeDesc = `双方握手言和 / 平局收场 (原因: ${status})`;
    } else {
      outcomeDesc = `${isBotWinner ? '你获胜' : winner ? '{{user}}获胜' : '双方平局'} (原因: ${status})`;
    }

    return `
====== 📊 【棋局全息复盘与心理战报】 ======
- 对弈双方：你 (${myColor === 'w' ? '执白先手' : '执黑后手'}) vs {{user}}
- 开局心态与难度：开局设定为 Lv.${initialLevel} (${moodDesc})${currentLevel !== initialLevel ? ` -> 盘中因逆风死磕临时认真飙升至 Lv.${currentLevel}` : ''}
- 最终真实结局：${outcomeDesc}
- 交战长度：共 ${totalTurns} 个回合
- 终局子力残存：你剩余 ${botScore} 分 vs 对方剩余 ${userScore} 分 (分差: ${diff > 0 ? `你领先 ${diff} 分` : diff < 0 ? `对方领先 ${Math.abs(diff)} 分` : '平分秋色'})
- 压迫感统计：你将军对方 ${userCheckCount} 次；对方将军你 ${botCheckCount} 次
- 盘中关键子力变动录：
  ${keyMoments.length > 0 ? keyMoments.join('\n  ') : '双方均未发生核心大子阵亡或升变'}
- 🧠 【你的盘中隐藏操作与心理小本本】：
  ${innerNotes.length > 0 ? innerNotes.join('\n  ') : '本局正常推进，未触发特殊放水或极端死磕事件'}
===========================================`.trim();
  }
}