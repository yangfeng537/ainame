// ============================================
// api/name.js — AI 起名接口（阶段1：单模型豆包/火山方舟）
// 练习三：AI 起名平台
// 流程：接收前端需求 -> 流式调用豆包 -> 边生成边转发给前端(SSE) -> 存 Supabase
// 密钥全部从环境变量读取，不写死在代码里
// ============================================
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// ---- 从环境变量读取配置 ----
const DOUBAO_API_KEY = process.env.DOUBAO_API_KEY;                                            // 火山方舟 API Key
const DOUBAO_BASE_URL = process.env.DOUBAO_BASE_URL || 'https://ark.cn-beijing.volces.com/api/v1'; // 火山方舟接口地址
const DOUBAO_MODEL = process.env.DOUBAO_MODEL || 'doubao-seed-2-1-turbo';                      // 使用的模型 ID
const SUPABASE_URL = process.env.SUPABASE_URL;                                                // Supabase 地址
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;                                      // Supabase 匿名密钥

module.exports = async function handler(req, res) {
  // 1) 只接受 POST 请求
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  // 2) 读取用户输入的起名需求
  const { requirement } = req.body || {};
  if (!requirement || !requirement.trim()) {
    return res.status(400).json({ error: '请输入起名需求' });
  }

  // 3) 组装提示词：要求输出 3 组名字 + 寓意解释
  const systemPrompt =
    '你是一位专业的起名专家，擅长结合文化内涵、寓意、音韵为不同对象起名。' +
    '回答必须使用简体中文，格式清晰。';
  const userPrompt =
    `用户的需求是：${requirement.trim()}。\n` +
    '请为这个需求提供 3 组名字方案，每组包含：\n' +
    '1. 名字\n' +
    '2. 寓意解释（一句话，说清含义和出处）\n' +
    '最后用一句简短的话总结整体起名思路。';

  // 4) 设置 SSE 响应头，告诉浏览器：下面要流式输出
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  try {
    // 5) 调用豆包（火山方舟）的 OpenAI 兼容接口
    //    （对应官方 curl 示例，只是把 curl 换成了 Node 的 fetch，stream 打开）
    const upstream = await fetch(`${DOUBAO_BASE_URL}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${DOUBAO_API_KEY}`,
      },
      body: JSON.stringify({
        model: DOUBAO_MODEL,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userPrompt },
        ],
        thinking: { type: 'disabled' }, // 关闭思考链，起名回答直接出
        stream: true, // 关键：打开流式输出
      }),
    });

    // 6) 上游报错时，把错误以 SSE 事件推给前端显示
    if (!upstream.ok) {
      const errText = (await upstream.text()).slice(0, 300);
      res.write(`event: error\ndata: ${JSON.stringify({ message: `模型调用失败（HTTP ${upstream.status}）：${errText}` })}\n\n`);
      return res.end();
    }

    // 7) 边接收上游的流、边原样转发给前端（实现"边生成边显示"）
    const reader = upstream.body.getReader();
    const decoder = new TextDecoder();
    let raw = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, { stream: true });
      raw += chunk;       // 同时攒一份完整内容，后面存数据库用
      res.write(chunk);   // 转发给浏览器
    }

    // 8) 流结束后，把完整结果保存到 Supabase（作业要求：需求、方案、模型）
    const nameResult = parseSSE(raw);
    if (SUPABASE_URL && SUPABASE_ANON_KEY) {
      try {
        const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
        const { error } = await supabase.from('name_history').insert({
          user_requirement: requirement.trim(),
          name_result: nameResult,
          model_name: DOUBAO_MODEL,
        });
        if (error) {
          res.write(`event: note\ndata: ${JSON.stringify({ message: `名字已生成，但保存历史失败：${error.message}` })}\n\n`);
        }
      } catch (e) {
        res.write(`event: note\ndata: ${JSON.stringify({ message: `名字已生成，但保存历史异常：${e.message}` })}\n\n`);
      }
    }
    res.write('event: done\ndata: {"saved":true}\n\n');
    res.end();
  } catch (err) {
    res.write(`event: error\ndata: ${JSON.stringify({ message: '服务器内部错误：' + err.message })}\n\n`);
    res.end();
  }
};

// 解析 OpenAI 兼容的 SSE 分片，拼出完整文本
function parseSSE(raw) {
  let text = '';
  for (const line of raw.split('\n')) {
    if (!line.startsWith('data: ')) continue;
    const data = line.slice(6);
    if (data === '[DONE]') continue;
    try {
      const json = JSON.parse(data);
      const delta = json.choices?.[0]?.delta?.content;
      if (delta) text += delta;
    } catch (_) {
      // 忽略解析不了的行
    }
  }
  return text.trim();
}
