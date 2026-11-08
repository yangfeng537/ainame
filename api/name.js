// ============================================
// api/name.js — AI 起名接口（阶段2：多模型切换 modelConfigMap）
// 练习三：AI 起名平台
// 加新模型 = 在 modelConfigMap 里加一项配置，调用代码一行都不用改
// 密钥全部从环境变量读取，不写死在代码里
// ============================================
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// ---- 模型配置表：每个模型的地址、密钥、模型名集中在这里 ----
const modelConfigMap = {
  doubao: {
    baseUrl: process.env.DOUBAO_BASE_URL || 'https://ark.cn-beijing.volces.com/api/v1',
    apiKey: process.env.DOUBAO_API_KEY,
    model: process.env.DOUBAO_MODEL || 'doubao-seed-2-1-turbo-260628',
    extraBody: { thinking: { type: 'disabled' } }, // 豆包2.1系：关闭思考链，回答直接出
  },
  deepseek: {
    baseUrl: process.env.DEEPSEEK_BASE_URL || 'https://api.deepseek.com/v1',
    apiKey: process.env.DEEPSEEK_API_KEY,
    model: process.env.DEEPSEEK_MODEL || 'deepseek-chat',
    extraBody: {},
  },
};

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

module.exports = async function handler(req, res) {
  // 1) 只接受 POST
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  // 2) 读取用户输入：起名需求 + 选择的模型
  const { requirement, model } = req.body || {};
  if (!requirement || !requirement.trim()) {
    return res.status(400).json({ error: '请输入起名需求' });
  }

  // 3) 按模型名查配置表，拿到这家模型的地址/密钥/模型 ID
  const config = modelConfigMap[model];
  if (!config) {
    return res.status(400).json({ error: `未知模型：${model}，可选：${Object.keys(modelConfigMap).join('、')}` });
  }
  if (!config.apiKey) {
    return res.status(400).json({ error: `模型 ${model} 还没配置密钥（环境变量缺失）` });
  }

  // 4) 组装提示词
  const systemPrompt =
    '你是一位专业的起名专家，擅长结合文化内涵、寓意、音韵为不同对象起名。' +
    '回答必须使用简体中文，格式清晰。';
  const userPrompt =
    `用户的需求是：${requirement.trim()}。\n` +
    '请为这个需求提供 3 组名字方案，每组包含：\n' +
    '1. 名字\n' +
    '2. 寓意解释（一句话，说清含义和出处）\n' +
    '最后用一句简短的话总结整体起名思路。';

  // 5) SSE 响应头：流式输出
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  try {
    // 6) 一套共用调用代码：换模型只是换配置表里的那一行
    const upstream = await fetch(`${config.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${config.apiKey}`,
      },
      body: JSON.stringify({
        model: config.model,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userPrompt },
        ],
        ...(config.extraBody || {}),
        stream: true,
      }),
    });

    if (!upstream.ok) {
      const errText = (await upstream.text()).slice(0, 300);
      res.write(`event: error\ndata: ${JSON.stringify({ message: `模型调用失败（HTTP ${upstream.status}）：${errText}` })}\n\n`);
      return res.end();
    }

    // 7) 边接收上游流、边转发给前端
    const reader = upstream.body.getReader();
    const decoder = new TextDecoder();
    let raw = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, { stream: true });
      raw += chunk;
      res.write(chunk);
    }

    // 8) 流结束后存 Supabase：需求 + 结果 + 模型
    const nameResult = parseSSE(raw);
    if (SUPABASE_URL && SUPABASE_ANON_KEY) {
      try {
        const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
        const { error } = await supabase.from('name_history').insert({
          user_requirement: requirement.trim(),
          name_result: nameResult,
          model_name: config.model,
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

// 解析 OpenAI 兼容 SSE 分片，拼出完整文本
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
    } catch (_) {}
  }
  return text.trim();
}
