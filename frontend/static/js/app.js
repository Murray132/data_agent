/**
 * DATA AGENT 前端应用
 * 
 * 功能模块：
 * 1. 数据表管理 - 查看表结构、数据预览、编辑元数据
 * 2. 元数据补全Agent - 调用AI生成元数据描述
 * 3. SQL生成Agent - 根据自然语言生成SQL
 */

// ============ API配置 ============
const API_BASE = '';  // 当前域名

// ============ 全局状态 ============
const state = {
    currentTable: null,
    tables: [],
    currentPage: 0,
    pageSize: 50,
    totalRows: 0,
    generatedMetadata: null,
    generatedSQL: null,
    // Tagging Agent状态
    generatedTags: null,
    existingTags: null,
    // SQL Validation状态
    validationResult: null,
    generatedTestSQL: null
};

// ============ 工具函数 ============

/**
 * 发送API请求
 */
async function fetchAPI(url, options = {}) {
    const defaultOptions = {
        headers: {
            'Content-Type': 'application/json'
        }
    };
    
    const response = await fetch(API_BASE + url, { ...defaultOptions, ...options });
    
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: '请求失败' }));
        throw new Error(error.detail || '请求失败');
    }
    
    return response.json();
}

/**
 * 显示加载遮罩
 */
function showLoading(text = '正在处理...') {
    document.getElementById('loading-text').textContent = text;
    document.getElementById('loading-overlay').classList.add('show');
}

/**
 * 隐藏加载遮罩
 */
function hideLoading() {
    document.getElementById('loading-overlay').classList.remove('show');
}

/**
 * 显示提示消息
 */
function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = 'toast show ' + type;
    
    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

/**
 * 显示弹窗
 */
function showModal(title, content, onConfirm) {
    document.getElementById('modal-title').textContent = title;
    document.getElementById('modal-body').innerHTML = content;
    document.getElementById('modal').classList.add('show');
    
    const confirmBtn = document.getElementById('modal-confirm');
    confirmBtn.onclick = () => {
        if (onConfirm) onConfirm();
        hideModal();
    };
}

/**
 * 隐藏弹窗
 */
function hideModal() {
    document.getElementById('modal').classList.remove('show');
}

// ============ 数据表管理模块 ============

/**
 * 加载数据库概览
 */
async function loadDatabaseSummary() {
    try {
        const data = await fetchAPI('/api/database/summary');
        
        document.getElementById('db-stats').innerHTML = `
            <div class="stat-item">
                <div class="label">数据表</div>
                <div class="value">${data.table_count}</div>
            </div>
            <div class="stat-item">
                <div class="label">总记录数</div>
                <div class="value">${data.total_rows.toLocaleString()}</div>
            </div>
            <div class="stat-item">
                <div class="label">总字段数</div>
                <div class="value">${data.total_columns}</div>
            </div>
        `;
    } catch (error) {
        console.error('加载数据库概览失败:', error);
    }
}

/**
 * 加载表列表
 */
async function loadTableList() {
    try {
        const data = await fetchAPI('/api/tables');
        state.tables = data.tables;
        
        renderTableList(state.tables);
        populateTableSelect(state.tables);
        populateTaggingTableSelect(state.tables);
        
    } catch (error) {
        console.error('加载表列表失败:', error);
        showToast('加载表列表失败', 'error');
    }
}

/**
 * 渲染表列表
 */
function renderTableList(tables) {
    const listEl = document.getElementById('table-list');
    
    listEl.innerHTML = tables.map(t => `
        <li class="table-item ${t.table_name === state.currentTable ? 'active' : ''}" 
            data-table="${t.table_name}">
            <div class="name">${t.table_name}</div>
            <div class="meta">${t.column_count}个字段 · ${t.row_count.toLocaleString()}条数据</div>
            ${t.description ? `<div class="desc">${t.description}</div>` : ''}
        </li>
    `).join('');
    
    // 绑定点击事件
    listEl.querySelectorAll('.table-item').forEach(item => {
        item.addEventListener('click', () => {
            const tableName = item.dataset.table;
            selectTable(tableName);
        });
    });
}

/**
 * 选择表
 */
async function selectTable(tableName) {
    state.currentTable = tableName;
    state.currentPage = 0;
    
    // 更新列表选中状态
    document.querySelectorAll('.table-item').forEach(item => {
        item.classList.toggle('active', item.dataset.table === tableName);
    });
    
    // 更新标题
    document.getElementById('detail-title').textContent = tableName;
    
    // 加载表详情
    await Promise.all([
        loadTableSchema(tableName),
        loadTableData(tableName),
        loadTableRelations(tableName)
    ]);
}

/**
 * 加载表结构
 */
async function loadTableSchema(tableName) {
    try {
        const schema = await fetchAPI(`/api/tables/${tableName}/schema`);
        
        // 渲染表描述
        const descEl = document.getElementById('table-description');
        descEl.innerHTML = `
            <div class="label">表描述</div>
            <div class="text">${schema.description || '<span style="color:#999">暂无描述</span>'}</div>
            <button class="edit-btn" onclick="editTableDescription('${tableName}', '${schema.description || ''}')">编辑</button>
        `;
        
        // 渲染字段列表
        const tbody = document.querySelector('#columns-table tbody');
        tbody.innerHTML = schema.columns.map(col => `
            <tr>
                <td><strong>${col.name}</strong></td>
                <td><code>${col.type}</code></td>
                <td>
                    ${col.is_primary_key ? '<span class="pk-badge">主键</span>' : ''}
                    ${col.not_null ? '<span class="notnull-badge">非空</span>' : ''}
                </td>
                <td>${col.description || '<span style="color:#999">暂无</span>'}</td>
                <td>
                    <button class="btn btn-small btn-secondary" 
                        onclick="editColumnDescription('${tableName}', '${col.name}', '${col.description || ''}')">
                        编辑
                    </button>
                </td>
            </tr>
        `).join('');
        
    } catch (error) {
        console.error('加载表结构失败:', error);
    }
}

/**
 * 加载表数据
 */
async function loadTableData(tableName, page = 0) {
    try {
        const offset = page * state.pageSize;
        const data = await fetchAPI(`/api/tables/${tableName}/data?limit=${state.pageSize}&offset=${offset}`);
        
        state.totalRows = data.total;
        state.currentPage = page;
        
        // 更新分页信息
        document.getElementById('data-info').textContent = `共 ${data.total.toLocaleString()} 条数据`;
        document.getElementById('page-info').textContent = `第 ${page + 1} / ${Math.ceil(data.total / state.pageSize)} 页`;
        
        // 渲染表头
        const thead = document.querySelector('#data-table thead');
        thead.innerHTML = `<tr>${data.columns.map(c => `<th>${c}</th>`).join('')}</tr>`;
        
        // 渲染数据
        const tbody = document.querySelector('#data-table tbody');
        tbody.innerHTML = data.data.map(row => `
            <tr>${data.columns.map(c => `<td>${formatCell(row[c])}</td>`).join('')}</tr>
        `).join('');
        
        // 更新分页按钮状态
        document.getElementById('prev-page').disabled = page === 0;
        document.getElementById('next-page').disabled = offset + state.pageSize >= data.total;
        
    } catch (error) {
        console.error('加载表数据失败:', error);
    }
}

/**
 * 格式化单元格数据
 */
function formatCell(value) {
    if (value === null || value === undefined) return '<span style="color:#999">NULL</span>';
    if (typeof value === 'string' && value.length > 50) return value.substring(0, 50) + '...';
    return String(value);
}

/**
 * 加载表关联关系
 */
async function loadTableRelations(tableName) {
    try {
        const relations = await fetchAPI(`/api/tables/${tableName}/related`);
        
        const contentEl = document.getElementById('relations-content');
        let html = '';
        
        if (relations.references.length > 0) {
            html += `
                <div class="relation-group">
                    <h4>该表引用的表</h4>
                    ${relations.references.map(r => `
                        <div class="relation-item">
                            <strong>${tableName}.${r.column}</strong>
                            <span class="arrow">→</span>
                            <strong>${r.referenced_table}.${r.referenced_column}</strong>
                        </div>
                    `).join('')}
                </div>
            `;
        }
        
        if (relations.referenced_by.length > 0) {
            html += `
                <div class="relation-group">
                    <h4>引用该表的表</h4>
                    ${relations.referenced_by.map(r => `
                        <div class="relation-item">
                            <strong>${r.table}.${r.column}</strong>
                            <span class="arrow">→</span>
                            <strong>${tableName}.${r.referenced_column}</strong>
                        </div>
                    `).join('')}
                </div>
            `;
        }
        
        if (!html) {
            html = '<p style="color:#999">该表没有外键关联关系</p>';
        }
        
        contentEl.innerHTML = html;
        
    } catch (error) {
        console.error('加载关联关系失败:', error);
    }
}

/**
 * 编辑表描述
 */
function editTableDescription(tableName, currentDesc) {
    showModal('编辑表描述', `
        <div class="form-group">
            <label>表名：${tableName}</label>
            <textarea id="edit-desc-input" class="form-textarea" rows="4">${currentDesc}</textarea>
        </div>
    `, async () => {
        const newDesc = document.getElementById('edit-desc-input').value;
        try {
            await fetchAPI(`/api/tables/${tableName}/description`, {
                method: 'PUT',
                body: JSON.stringify({ description: newDesc })
            });
            showToast('更新成功', 'success');
            await loadTableSchema(tableName);
            await loadTableList();
        } catch (error) {
            showToast('更新失败: ' + error.message, 'error');
        }
    });
}

/**
 * 编辑字段描述
 */
function editColumnDescription(tableName, columnName, currentDesc) {
    showModal('编辑字段描述', `
        <div class="form-group">
            <label>字段：${tableName}.${columnName}</label>
            <textarea id="edit-desc-input" class="form-textarea" rows="4">${currentDesc}</textarea>
        </div>
    `, async () => {
        const newDesc = document.getElementById('edit-desc-input').value;
        try {
            await fetchAPI(`/api/tables/${tableName}/columns/${columnName}/description`, {
                method: 'PUT',
                body: JSON.stringify({ description: newDesc })
            });
            showToast('更新成功', 'success');
            await loadTableSchema(tableName);
        } catch (error) {
            showToast('更新失败: ' + error.message, 'error');
        }
    });
}

// ============ 元数据补全Agent模块 ============

/**
 * 填充表选择下拉框
 */
function populateTableSelect(tables) {
    const select = document.getElementById('metadata-table-select');
    select.innerHTML = '<option value="">-- 请选择 --</option>' + 
        tables.map(t => `<option value="${t.table_name}">${t.table_name} (${t.row_count}条)</option>`).join('');
}

/**
 * 加载缺少元数据的表
 */
async function loadMissingMetadata() {
    try {
        const missing = await fetchAPI('/api/metadata/missing');
        
        const listEl = document.getElementById('missing-metadata-list');
        
        if (missing.length === 0) {
            listEl.innerHTML = '<li style="color:#52c41a">所有表和字段都有元数据描述</li>';
            return;
        }
        
        listEl.innerHTML = missing.map(m => `
            <li onclick="document.getElementById('metadata-table-select').value='${m.table_name}'">
                <strong>${m.table_name}</strong>
                ${m.missing_table_description ? '(缺少表描述)' : ''}
                ${m.missing_column_descriptions.length > 0 ? `(${m.missing_column_descriptions.length}个字段缺少描述)` : ''}
            </li>
        `).join('');
        
    } catch (error) {
        console.error('加载缺少元数据信息失败:', error);
    }
}

/**
 * 生成元数据（流式输出）
 */
async function generateMetadata() {
    const tableName = document.getElementById('metadata-table-select').value;
    
    if (!tableName) {
        showToast('请先选择一个数据表', 'error');
        return;
    }
    
    // 显示思考过程区域
    const resultEl = document.getElementById('metadata-result');
    resultEl.innerHTML = `
        <div class="thinking-process" id="metadata-thinking">
            <h4>🤖 AI 执行过程</h4>
            <div class="steps-container" id="metadata-steps"></div>
            <div class="thinking-content" id="metadata-thinking-content"></div>
        </div>
        <div class="final-result" id="metadata-final-result" style="display:none;"></div>
    `;
    document.getElementById('metadata-actions').style.display = 'none';
    
    const stepsEl = document.getElementById('metadata-steps');
    const thinkingEl = document.getElementById('metadata-thinking-content');
    const finalResultEl = document.getElementById('metadata-final-result');
    
    try {
        // 使用 SSE 流式请求
        const response = await fetch('/api/agent/metadata/generate/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ table_name: tableName })
        });
        
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop(); // 保留不完整的行
            
            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    try {
                        const event = JSON.parse(line.slice(6));
                        handleMetadataStreamEvent(event, stepsEl, thinkingEl, finalResultEl);
                    } catch (e) {
                        console.error('解析事件失败:', e);
                    }
                }
            }
        }
        
        showToast('元数据生成完成', 'success');
        
    } catch (error) {
        showToast('生成失败: ' + error.message, 'error');
        thinkingEl.innerHTML += `<div class="error-message">❌ 错误: ${error.message}</div>`;
    }
}

/**
 * 处理元数据流式事件
 */
function handleMetadataStreamEvent(event, stepsEl, thinkingEl, finalResultEl) {
    switch (event.type) {
        case 'start':
            thinkingEl.innerHTML += `<div class="thinking-item">🚀 ${event.message}</div>`;
            break;
        
        case 'iteration_start':
            // 迭代开始 - 显示迭代分隔
            thinkingEl.innerHTML += `
                <div class="iteration-header">
                    <span class="iteration-badge">迭代 ${event.iteration}</span>
                    <span class="iteration-desc">${event.message}</span>
                </div>
            `;
            break;
        
        case 'missing_analysis':
            // 缺失元数据分析结果
            const missingTableBadge = event.missing_table_description ? 
                '<span class="missing-badge">表描述缺失</span>' : '';
            const missingColCount = event.missing_columns ? event.missing_columns.length : 0;
            const missingColBadge = missingColCount > 0 ? 
                `<span class="missing-badge">${missingColCount}个字段缺失描述</span>` : '';
            const missingColList = missingColCount > 0 ? 
                `<div class="missing-columns">缺失字段: ${event.missing_columns.slice(0, 10).join(', ')}${missingColCount > 10 ? '...' : ''}</div>` : '';
            
            thinkingEl.innerHTML += `
                <div class="missing-analysis">
                    <div class="missing-title">📋 元数据缺失分析</div>
                    <div class="missing-badges">${missingTableBadge}${missingColBadge}</div>
                    ${missingColList}
                </div>
            `;
            break;
        
        case 'skill_start':
            // Skill调用开始 - 显示分组标签
            addSkillLabel(stepsEl, event.skill_name, event.description || '');
            thinkingEl.innerHTML += `<div class="thinking-item">🔧 正在使用技能: ${event.skill_name}</div>`;
            break;
            
        case 'step':
            updateStepStatus(stepsEl, event);
            break;
            
        case 'tool_result':
            // 显示工具调用详情，包含入参和出参
            const skillBadge = event.is_skill_tool ? 
                `<span class="skill-badge">Skill: ${event.skill_name || 'Database Schema Analysis'}</span>` : '';
            const inputParams = event.input ? 
                `<div class="tool-input"><strong>入参:</strong> <code>${escapeHtml(JSON.stringify(event.input))}</code></div>` : '';
            
            thinkingEl.innerHTML += `
                <div class="tool-result ${event.is_skill_tool ? 'skill-tool' : ''}">
                    <div class="tool-name">📦 工具调用: ${event.tool} ${skillBadge}</div>
                    ${inputParams}
                    <div class="tool-output-label"><strong>出参:</strong></div>
                    <pre class="tool-output">${escapeHtml(event.result)}</pre>
                </div>
            `;
            // 自动滚动到底部
            thinkingEl.scrollTop = thinkingEl.scrollHeight;
            break;
            
        case 'thinking':
            thinkingEl.innerHTML += `<div class="thinking-item">💭 ${event.message}</div>`;
            break;
            
        case 'llm_response':
            thinkingEl.innerHTML += `
                <div class="llm-response">
                    <div class="llm-label">🤖 大模型响应:</div>
                    <pre class="llm-output">${escapeHtml(event.content)}</pre>
                </div>
            `;
            thinkingEl.scrollTop = thinkingEl.scrollHeight;
            break;
            
        case 'result':
            state.generatedMetadata = event.data;
            finalResultEl.style.display = 'block';
            
            if (event.data.parse_error) {
                finalResultEl.innerHTML = `
                    <h4>⚠️ 解析结果时出现问题</h4>
                    <p>请查看上方的AI响应内容</p>
                `;
            } else {
                finalResultEl.innerHTML = `
                    <h4>✅ 生成结果</h4>
                    <div class="result-section">
                        <h5>表描述</h5>
                        <p>${event.data.table_description || '未生成'}</p>
                    </div>
                    <div class="result-section">
                        <h5>字段描述</h5>
                        <ul>
                            ${Object.entries(event.data.column_descriptions || {}).map(([col, desc]) => 
                                `<li><strong>${col}</strong>: ${desc}</li>`
                            ).join('')}
                        </ul>
                    </div>
                `;
                document.getElementById('metadata-actions').style.display = 'flex';
            }
            break;
            
        case 'error':
            thinkingEl.innerHTML += `<div class="error-message">❌ 错误: ${event.message}</div>`;
            break;
            
        case 'end':
            thinkingEl.innerHTML += `<div class="thinking-item">✨ ${event.message}</div>`;
            break;
    }
}

/**
 * 更新步骤状态（网格样式）
 */
function updateStepStatus(container, event) {
    // 使用容器ID前缀避免不同模块间ID冲突
    const containerId = container.id || 'default';
    const stepId = `${containerId}-step-${event.step}`;
    let stepEl = document.getElementById(stepId);
    
    if (!stepEl) {
        stepEl = document.createElement('div');
        stepEl.id = stepId;
        stepEl.className = 'step-card';
        container.appendChild(stepEl);
    }
    
    const statusIcon = event.status === 'done' ? '✓' : 
                       event.status === 'running' ? '◉' : '○';
    const statusText = event.status === 'done' ? '已完成' : 
                       event.status === 'running' ? '运行中' : '待执行';
    
    stepEl.className = `step-card step-${event.status}`;
    stepEl.innerHTML = `
        <div class="step-card-header">
            <span class="step-status-indicator">${statusIcon}</span>
            <span class="step-title">${event.title}</span>
        </div>
        <div class="step-message">${event.message}</div>
        <div class="step-status">${statusText}</div>
    `;
}

/**
 * 添加Skill标签
 */
function addSkillLabel(container, skillName, description) {
    const skillLabel = document.createElement('div');
    skillLabel.className = 'skill-label';
    skillLabel.innerHTML = `
        <span class="skill-icon">🔧</span>
        <span>正在使用 ${skillName} 技能</span>
    `;
    container.appendChild(skillLabel);
}

/**
 * HTML转义
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * 应用元数据到数据库
 */
async function applyMetadata() {
    if (!state.generatedMetadata) return;
    
    showLoading('正在应用元数据...');
    
    try {
        const result = await fetchAPI('/api/agent/metadata/apply', {
            method: 'POST',
            body: JSON.stringify({
                table_name: state.generatedMetadata.table_name,
                table_description: state.generatedMetadata.table_description,
                column_descriptions: state.generatedMetadata.column_descriptions
            })
        });
        
        showToast('元数据已成功应用到数据库', 'success');
        
        // 刷新数据
        await loadTableList();
        await loadMissingMetadata();
        
        if (state.currentTable === state.generatedMetadata.table_name) {
            await loadTableSchema(state.currentTable);
        }
        
    } catch (error) {
        showToast('应用失败: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

/**
 * 复制元数据
 */
function copyMetadata() {
    if (!state.generatedMetadata) return;
    
    const text = JSON.stringify(state.generatedMetadata, null, 2);
    navigator.clipboard.writeText(text).then(() => {
        showToast('已复制到剪贴板', 'success');
    });
}

// ============ SQL生成Agent模块 ============

/**
 * 生成SQL（流式输出）
 */
async function generateSQL() {
    const requirement = document.getElementById('sql-requirement').value.trim();
    const context = document.getElementById('sql-context').value.trim();
    
    if (!requirement) {
        showToast('请输入查询需求', 'error');
        return;
    }
    
    // 显示思考过程区域
    const resultEl = document.getElementById('sql-result');
    resultEl.innerHTML = `
        <div class="thinking-process" id="sql-thinking">
            <h4>🤖 AI 执行过程</h4>
            <div class="steps-container" id="sql-steps"></div>
            <div class="thinking-content" id="sql-thinking-content"></div>
        </div>
        <div class="final-result" id="sql-final-result" style="display:none;"></div>
    `;
    document.getElementById('sql-actions').style.display = 'none';
    document.getElementById('sql-execution-result').style.display = 'none';
    
    const stepsEl = document.getElementById('sql-steps');
    const thinkingEl = document.getElementById('sql-thinking-content');
    const finalResultEl = document.getElementById('sql-final-result');
    
    try {
        // 使用 SSE 流式请求
        const response = await fetch('/api/agent/sql/generate/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ requirement, context: context || null })
        });
        
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop();
            
            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    try {
                        const event = JSON.parse(line.slice(6));
                        handleSQLStreamEvent(event, stepsEl, thinkingEl, finalResultEl);
                    } catch (e) {
                        console.error('解析事件失败:', e);
                    }
                }
            }
        }
        
        showToast('SQL生成完成', 'success');
        
    } catch (error) {
        showToast('生成失败: ' + error.message, 'error');
        thinkingEl.innerHTML += `<div class="error-message">❌ 错误: ${error.message}</div>`;
    }
}

/**
 * 处理SQL流式事件
 */
function handleSQLStreamEvent(event, stepsEl, thinkingEl, finalResultEl) {
    switch (event.type) {
        case 'start':
            thinkingEl.innerHTML += `<div class="thinking-item">🚀 ${event.message}</div>`;
            break;
        
        case 'iteration_start':
            // 迭代开始 - 显示迭代分隔
            thinkingEl.innerHTML += `
                <div class="iteration-header">
                    <span class="iteration-badge">迭代 ${event.iteration}</span>
                    <span class="iteration-desc">${event.message}</span>
                </div>
            `;
            break;
        
        case 'skill_start':
            // Skill调用开始 - 显示分组标签
            addSkillLabel(stepsEl, event.skill_name, event.description || '');
            thinkingEl.innerHTML += `<div class="thinking-item">🔧 正在使用技能: ${event.skill_name}</div>`;
            break;
            
        case 'step':
            updateStepStatus(stepsEl, event);
            break;
            
        case 'tool_result':
            // 显示工具调用详情，包含入参和出参
            const sqlSkillBadge = event.is_skill_tool ? 
                `<span class="skill-badge">Skill: ${event.skill_name || 'Database Schema Analysis'}</span>` : '';
            const sqlInputParams = event.input ? 
                `<div class="tool-input"><strong>入参:</strong> <code>${escapeHtml(JSON.stringify(event.input))}</code></div>` : '';
            
            thinkingEl.innerHTML += `
                <div class="tool-result ${event.is_skill_tool ? 'skill-tool' : ''}">
                    <div class="tool-name">📦 工具调用: ${event.tool} ${sqlSkillBadge}</div>
                    ${sqlInputParams}
                    <div class="tool-output-label"><strong>出参:</strong></div>
                    <pre class="tool-output">${escapeHtml(event.result)}</pre>
                </div>
            `;
            thinkingEl.scrollTop = thinkingEl.scrollHeight;
            break;
            
        case 'thinking':
            thinkingEl.innerHTML += `<div class="thinking-item">💭 ${event.message}</div>`;
            break;
            
        case 'llm_response':
            thinkingEl.innerHTML += `
                <div class="llm-response">
                    <div class="llm-label">🤖 大模型响应:</div>
                    <pre class="llm-output">${escapeHtml(event.content)}</pre>
                </div>
            `;
            thinkingEl.scrollTop = thinkingEl.scrollHeight;
            break;
            
        case 'result':
            state.generatedSQL = event.data;
            finalResultEl.style.display = 'block';
            
            if (event.data.sql) {
                finalResultEl.innerHTML = `
                    <h4>✅ 生成的SQL</h4>
                    <div class="sql-code">
                        <pre>${escapeHtml(event.data.sql)}</pre>
                    </div>
                    <div class="explanation">
                        <h5>说明</h5>
                        <p>${event.data.explanation || '无'}</p>
                        ${event.data.tables_used && event.data.tables_used.length > 0 ? 
                            `<p><strong>涉及的表：</strong>${event.data.tables_used.join(', ')}</p>` : ''}
                        ${event.data.key_points && event.data.key_points.length > 0 ?
                            `<h5>要点</h5><ul>${event.data.key_points.map(p => `<li>${p}</li>`).join('')}</ul>` : ''}
                    </div>
                `;
                document.getElementById('sql-actions').style.display = 'flex';
            } else {
                finalResultEl.innerHTML = `
                    <h4>⚠️ 未能生成SQL</h4>
                    <p>请查看上方的AI响应内容，或尝试更详细地描述需求</p>
                `;
            }
            break;
            
        case 'error':
            thinkingEl.innerHTML += `<div class="error-message">❌ 错误: ${event.message}</div>`;
            break;
            
        case 'end':
            thinkingEl.innerHTML += `<div class="thinking-item">✨ ${event.message}</div>`;
            break;
    }
}

/**
 * 执行SQL
 */
async function executeSQL() {
    if (!state.generatedSQL || !state.generatedSQL.sql) return;
    
    showLoading('正在执行SQL...');
    
    try {
        const result = await fetchAPI('/api/sql/execute', {
            method: 'POST',
            body: JSON.stringify({ sql: state.generatedSQL.sql })
        });
        
        // 渲染执行结果
        const resultSection = document.getElementById('sql-execution-result');
        resultSection.style.display = 'block';
        
        const thead = document.querySelector('#sql-result-table thead');
        const tbody = document.querySelector('#sql-result-table tbody');
        
        if (result.data && result.data.length > 0) {
            thead.innerHTML = `<tr>${result.columns.map(c => `<th>${c}</th>`).join('')}</tr>`;
            tbody.innerHTML = result.data.map(row => `
                <tr>${result.columns.map(c => `<td>${formatCell(row[c])}</td>`).join('')}</tr>
            `).join('');
            
            showToast(`执行成功，返回 ${result.row_count} 条结果`, 'success');
        } else {
            thead.innerHTML = '';
            tbody.innerHTML = '<tr><td>查询成功，但没有返回数据</td></tr>';
            showToast('查询成功，没有返回数据', 'success');
        }
        
    } catch (error) {
        showToast('执行失败: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

/**
 * 复制SQL
 */
function copySQL() {
    if (!state.generatedSQL || !state.generatedSQL.sql) return;
    
    navigator.clipboard.writeText(state.generatedSQL.sql).then(() => {
        showToast('SQL已复制到剪贴板', 'success');
    });
}

/**
 * 解释SQL
 */
async function explainSQL() {
    if (!state.generatedSQL || !state.generatedSQL.sql) return;
    
    showLoading('AI正在分析SQL...');
    
    try {
        const result = await fetchAPI('/api/agent/sql/explain', {
            method: 'POST',
            body: JSON.stringify({ sql: state.generatedSQL.sql })
        });
        
        showModal('SQL解释', `
            <div style="white-space: pre-wrap; font-size: 14px; line-height: 1.8;">
                ${result.explanation}
            </div>
        `);
        
    } catch (error) {
        showToast('解释失败: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

// ============ ER图模块 ============

// ER图状态
const erState = {
    nodes: [],
    edges: [],
    canvas: null,
    ctx: null,
    scale: 1,
    offsetX: 0,
    offsetY: 0,
    isDragging: false,
    dragStart: { x: 0, y: 0 },
    nodePositions: {},
    selectedNode: null
};

/**
 * 加载ER图数据
 */
async function loadERDiagram() {
    try {
        const data = await fetchAPI('/api/database/er-diagram');
        erState.nodes = data.nodes;
        erState.edges = data.edges;
        
        // 计算节点位置（使用力导向布局的简化版本）
        calculateNodePositions();
        
        // 初始化画布并绘制
        initERCanvas();
        drawERDiagram();
        
    } catch (error) {
        console.error('加载ER图失败:', error);
        document.getElementById('er-diagram').innerHTML = 
            '<p style="text-align:center;color:#999;padding:50px;">加载ER图失败</p>';
    }
}

/**
 * 计算节点位置（圆形布局）
 */
function calculateNodePositions() {
    const nodes = erState.nodes;
    const count = nodes.length;
    const centerX = 600;
    const centerY = 350;
    const radius = Math.min(400, 100 + count * 15);
    
    nodes.forEach((node, i) => {
        const angle = (2 * Math.PI * i) / count - Math.PI / 2;
        erState.nodePositions[node.id] = {
            x: centerX + radius * Math.cos(angle),
            y: centerY + radius * Math.sin(angle),
            width: 180,
            height: 40 + Math.min(node.columns.length, 8) * 18
        };
    });
}

/**
 * 初始化ER画布
 */
function initERCanvas() {
    const container = document.getElementById('er-diagram');
    container.innerHTML = '<canvas id="er-canvas"></canvas>';
    
    const canvas = document.getElementById('er-canvas');
    canvas.width = container.clientWidth || 1200;
    canvas.height = 700;
    
    erState.canvas = canvas;
    erState.ctx = canvas.getContext('2d');
    
    // 鼠标事件
    canvas.addEventListener('mousedown', handleERMouseDown);
    canvas.addEventListener('mousemove', handleERMouseMove);
    canvas.addEventListener('mouseup', handleERMouseUp);
    canvas.addEventListener('wheel', handleERWheel);
    canvas.addEventListener('dblclick', handleERDoubleClick);
}

/**
 * 绘制ER图
 */
function drawERDiagram() {
    const ctx = erState.ctx;
    const canvas = erState.canvas;
    
    // 清空画布
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    
    // 应用变换
    ctx.save();
    ctx.translate(erState.offsetX, erState.offsetY);
    ctx.scale(erState.scale, erState.scale);
    
    // 绘制连线（先画线，再画节点）
    drawEREdges();
    
    // 绘制节点
    drawERNodes();
    
    ctx.restore();
}

/**
 * 绘制连线
 */
function drawEREdges() {
    const ctx = erState.ctx;
    
    erState.edges.forEach(edge => {
        const fromPos = erState.nodePositions[edge.from];
        const toPos = erState.nodePositions[edge.to];
        
        if (!fromPos || !toPos) return;
        
        // 计算连线起点和终点
        const fromX = fromPos.x + fromPos.width / 2;
        const fromY = fromPos.y + fromPos.height / 2;
        const toX = toPos.x + toPos.width / 2;
        const toY = toPos.y + toPos.height / 2;
        
        // 绘制曲线
        ctx.beginPath();
        ctx.strokeStyle = '#1890ff';
        ctx.lineWidth = 2;
        
        // 使用贝塞尔曲线
        const midX = (fromX + toX) / 2;
        const midY = (fromY + toY) / 2;
        const ctrlX = midX;
        const ctrlY = midY - 30;
        
        ctx.moveTo(fromX, fromY);
        ctx.quadraticCurveTo(ctrlX, ctrlY, toX, toY);
        ctx.stroke();
        
        // 绘制箭头
        const angle = Math.atan2(toY - ctrlY, toX - ctrlX);
        ctx.beginPath();
        ctx.fillStyle = '#1890ff';
        ctx.moveTo(toX, toY);
        ctx.lineTo(toX - 10 * Math.cos(angle - Math.PI / 6), toY - 10 * Math.sin(angle - Math.PI / 6));
        ctx.lineTo(toX - 10 * Math.cos(angle + Math.PI / 6), toY - 10 * Math.sin(angle + Math.PI / 6));
        ctx.closePath();
        ctx.fill();
    });
}

/**
 * 绘制节点
 */
function drawERNodes() {
    const ctx = erState.ctx;
    
    erState.nodes.forEach(node => {
        const pos = erState.nodePositions[node.id];
        if (!pos) return;
        
        const { x, y, width, height } = pos;
        const isSelected = erState.selectedNode === node.id;
        
        // 绘制表框
        ctx.fillStyle = isSelected ? '#e6f7ff' : '#ffffff';
        ctx.strokeStyle = isSelected ? '#1890ff' : '#d9d9d9';
        ctx.lineWidth = isSelected ? 2 : 1;
        
        // 圆角矩形
        const radius = 6;
        ctx.beginPath();
        ctx.moveTo(x + radius, y);
        ctx.lineTo(x + width - radius, y);
        ctx.quadraticCurveTo(x + width, y, x + width, y + radius);
        ctx.lineTo(x + width, y + height - radius);
        ctx.quadraticCurveTo(x + width, y + height, x + width - radius, y + height);
        ctx.lineTo(x + radius, y + height);
        ctx.quadraticCurveTo(x, y + height, x, y + height - radius);
        ctx.lineTo(x, y + radius);
        ctx.quadraticCurveTo(x, y, x + radius, y);
        ctx.closePath();
        ctx.fill();
        ctx.stroke();
        
        // 绘制表头
        ctx.fillStyle = '#1890ff';
        ctx.beginPath();
        ctx.moveTo(x + radius, y);
        ctx.lineTo(x + width - radius, y);
        ctx.quadraticCurveTo(x + width, y, x + width, y + radius);
        ctx.lineTo(x + width, y + 28);
        ctx.lineTo(x, y + 28);
        ctx.lineTo(x, y + radius);
        ctx.quadraticCurveTo(x, y, x + radius, y);
        ctx.closePath();
        ctx.fill();
        
        // 表名
        ctx.fillStyle = '#ffffff';
        ctx.font = 'bold 12px Arial';
        ctx.textAlign = 'center';
        ctx.fillText(node.id, x + width / 2, y + 18);
        
        // 绘制字段（最多显示8个）
        ctx.font = '11px Arial';
        ctx.textAlign = 'left';
        const displayCols = node.columns.slice(0, 8);
        
        displayCols.forEach((col, i) => {
            const colY = y + 44 + i * 18;
            
            // 字段图标
            if (col.is_pk) {
                ctx.fillStyle = '#faad14';
                ctx.fillText('🔑', x + 6, colY);
            } else if (col.is_fk) {
                ctx.fillStyle = '#52c41a';
                ctx.fillText('🔗', x + 6, colY);
            } else {
                ctx.fillStyle = '#999';
                ctx.fillText('○', x + 8, colY);
            }
            
            // 字段名
            ctx.fillStyle = '#333';
            const colName = col.name.length > 15 ? col.name.slice(0, 15) + '...' : col.name;
            ctx.fillText(colName, x + 24, colY);
        });
        
        // 如果有更多字段
        if (node.columns.length > 8) {
            ctx.fillStyle = '#999';
            ctx.font = '10px Arial';
            ctx.fillText(`... +${node.columns.length - 8} 个字段`, x + 24, y + height - 6);
        }
    });
}

/**
 * 鼠标按下事件
 */
function handleERMouseDown(e) {
    const rect = erState.canvas.getBoundingClientRect();
    const x = (e.clientX - rect.left - erState.offsetX) / erState.scale;
    const y = (e.clientY - rect.top - erState.offsetY) / erState.scale;
    
    // 检查是否点击了节点
    for (const node of erState.nodes) {
        const pos = erState.nodePositions[node.id];
        if (x >= pos.x && x <= pos.x + pos.width && y >= pos.y && y <= pos.y + pos.height) {
            erState.selectedNode = node.id;
            erState.isDragging = true;
            erState.dragStart = { x: x - pos.x, y: y - pos.y };
            drawERDiagram();
            return;
        }
    }
    
    // 拖拽画布
    erState.isDragging = true;
    erState.dragStart = { x: e.clientX - erState.offsetX, y: e.clientY - erState.offsetY };
    erState.selectedNode = null;
    drawERDiagram();
}

/**
 * 鼠标移动事件
 */
function handleERMouseMove(e) {
    if (!erState.isDragging) return;
    
    const rect = erState.canvas.getBoundingClientRect();
    
    if (erState.selectedNode) {
        // 拖拽节点
        const x = (e.clientX - rect.left - erState.offsetX) / erState.scale;
        const y = (e.clientY - rect.top - erState.offsetY) / erState.scale;
        
        erState.nodePositions[erState.selectedNode].x = x - erState.dragStart.x;
        erState.nodePositions[erState.selectedNode].y = y - erState.dragStart.y;
    } else {
        // 拖拽画布
        erState.offsetX = e.clientX - erState.dragStart.x;
        erState.offsetY = e.clientY - erState.dragStart.y;
    }
    
    drawERDiagram();
}

/**
 * 鼠标松开事件
 */
function handleERMouseUp() {
    erState.isDragging = false;
}

/**
 * 滚轮缩放事件
 */
function handleERWheel(e) {
    e.preventDefault();
    const delta = e.deltaY > 0 ? 0.9 : 1.1;
    erState.scale = Math.max(0.3, Math.min(2, erState.scale * delta));
    drawERDiagram();
}

/**
 * 双击查看表详情
 */
function handleERDoubleClick(e) {
    const rect = erState.canvas.getBoundingClientRect();
    const x = (e.clientX - rect.left - erState.offsetX) / erState.scale;
    const y = (e.clientY - rect.top - erState.offsetY) / erState.scale;
    
    for (const node of erState.nodes) {
        const pos = erState.nodePositions[node.id];
        if (x >= pos.x && x <= pos.x + pos.width && y >= pos.y && y <= pos.y + pos.height) {
            // 点击了某个表，切换到该表的详情
            selectTable(node.id);
            return;
        }
    }
}

/**
 * 重置ER图视图
 */
function resetERDiagram() {
    erState.scale = 1;
    erState.offsetX = 0;
    erState.offsetY = 0;
    calculateNodePositions();
    drawERDiagram();
}

/**
 * 切换ER图全屏
 */
function toggleERFullscreen() {
    const container = document.querySelector('.er-diagram-container');
    
    if (container.classList.contains('fullscreen')) {
        container.classList.remove('fullscreen');
        erState.canvas.width = container.clientWidth;
        erState.canvas.height = 700;
    } else {
        container.classList.add('fullscreen');
        erState.canvas.width = window.innerWidth - 40;
        erState.canvas.height = window.innerHeight - 100;
    }
    
    drawERDiagram();
}

// ============ 数据资产打标Agent模块 ============

/**
 * 填充打标表选择下拉框
 */
function populateTaggingTableSelect(tables) {
    const select = document.getElementById('tagging-table-select');
    if (!select) return;
    select.innerHTML = '<option value="">-- 请选择 --</option>' + 
        tables.map(t => `<option value="${t.table_name}">${t.table_name} (${t.row_count}条)</option>`).join('');
}

/**
 * 加载表的现有标签
 */
async function loadExistingTags(tableName) {
    if (!tableName) {
        document.getElementById('existing-tags-panel').style.display = 'none';
        return;
    }
    
    try {
        const data = await fetchAPI(`/api/tables/${tableName}/tags`);
        state.existingTags = data;
        
        // 显示标签面板
        document.getElementById('existing-tags-panel').style.display = 'block';
        
        // 渲染表标签
        renderTableTags(tableName, data.table_tags || []);
        
        // 渲染字段标签
        renderColumnTags(tableName, data.column_tags || {});
        
    } catch (error) {
        console.error('加载现有标签失败:', error);
        document.getElementById('existing-tags-panel').style.display = 'none';
    }
}

/**
 * 渲染表标签
 */
function renderTableTags(tableName, tags) {
    const container = document.getElementById('table-tags');
    
    let html = tags.map(tag => `
        <span class="tag tag-${tag.created_by}">
            ${tag.tag}
            <button class="tag-delete" onclick="deleteTableTag('${tableName}', '${tag.tag}')">&times;</button>
        </span>
    `).join('');
    
    // 添加新标签输入框
    html += `
        <span class="tag-input-wrapper">
            <input type="text" class="tag-input" id="new-table-tag" placeholder="添加标签..." 
                onkeypress="handleTableTagInput(event, '${tableName}')">
            <button class="tag-add-btn" onclick="addTableTag('${tableName}')">+</button>
        </span>
    `;
    
    container.innerHTML = html;
}

/**
 * 渲染字段标签
 */
function renderColumnTags(tableName, columnTags) {
    const container = document.getElementById('column-tags-list');
    
    if (Object.keys(columnTags).length === 0) {
        container.innerHTML = '<p class="no-tags">暂无字段标签</p>';
        return;
    }
    
    let html = '';
    for (const [columnName, tags] of Object.entries(columnTags)) {
        html += `
            <div class="column-tags-item">
                <span class="column-name">${columnName}</span>
                <div class="tags-wrapper">
                    ${tags.map(tag => `
                        <span class="tag tag-${tag.created_by}">
                            ${tag.tag}
                            <button class="tag-delete" onclick="deleteColumnTag('${tableName}', '${columnName}', '${tag.tag}')">&times;</button>
                        </span>
                    `).join('')}
                    <span class="tag-input-wrapper">
                        <input type="text" class="tag-input tag-input-small" 
                            id="new-col-tag-${columnName}" placeholder="+"
                            onkeypress="handleColumnTagInput(event, '${tableName}', '${columnName}')">
                    </span>
                </div>
            </div>
        `;
    }
    
    container.innerHTML = html;
}

/**
 * 处理表标签输入回车
 */
function handleTableTagInput(event, tableName) {
    if (event.key === 'Enter') {
        addTableTag(tableName);
    }
}

/**
 * 添加表标签
 */
async function addTableTag(tableName) {
    const input = document.getElementById('new-table-tag');
    const tag = input.value.trim();
    
    if (!tag) return;
    
    try {
        await fetchAPI(`/api/tables/${tableName}/tags`, {
            method: 'POST',
            body: JSON.stringify({ tag: tag, action: 'add' })
        });
        
        input.value = '';
        showToast('标签添加成功', 'success');
        await loadExistingTags(tableName);
        
    } catch (error) {
        showToast('添加失败: ' + error.message, 'error');
    }
}

/**
 * 删除表标签
 */
async function deleteTableTag(tableName, tag) {
    try {
        await fetchAPI(`/api/tables/${tableName}/tags`, {
            method: 'POST',
            body: JSON.stringify({ tag: tag, action: 'delete' })
        });
        
        showToast('标签已删除', 'success');
        await loadExistingTags(tableName);
        
    } catch (error) {
        showToast('删除失败: ' + error.message, 'error');
    }
}

/**
 * 处理字段标签输入回车
 */
function handleColumnTagInput(event, tableName, columnName) {
    if (event.key === 'Enter') {
        addColumnTag(tableName, columnName);
    }
}

/**
 * 添加字段标签
 */
async function addColumnTag(tableName, columnName) {
    const input = document.getElementById(`new-col-tag-${columnName}`);
    const tag = input.value.trim();
    
    if (!tag) return;
    
    try {
        await fetchAPI(`/api/tables/${tableName}/tags`, {
            method: 'POST',
            body: JSON.stringify({ tag: tag, action: 'add', column_name: columnName })
        });
        
        input.value = '';
        showToast('标签添加成功', 'success');
        await loadExistingTags(tableName);
        
    } catch (error) {
        showToast('添加失败: ' + error.message, 'error');
    }
}

/**
 * 删除字段标签
 */
async function deleteColumnTag(tableName, columnName, tag) {
    try {
        await fetchAPI(`/api/tables/${tableName}/tags`, {
            method: 'POST',
            body: JSON.stringify({ tag: tag, action: 'delete', column_name: columnName })
        });
        
        showToast('标签已删除', 'success');
        await loadExistingTags(tableName);
        
    } catch (error) {
        showToast('删除失败: ' + error.message, 'error');
    }
}

/**
 * 生成标签（流式输出）
 */
async function generateTags() {
    const tableName = document.getElementById('tagging-table-select').value;
    
    if (!tableName) {
        showToast('请先选择一个数据表', 'error');
        return;
    }
    
    // 显示思考过程区域
    const resultEl = document.getElementById('tagging-result');
    resultEl.innerHTML = `
        <div class="thinking-process" id="tagging-thinking">
            <h4>🏷️ AI 标签生成过程</h4>
            <div class="steps-container" id="tagging-steps"></div>
            <div class="thinking-content" id="tagging-thinking-content"></div>
        </div>
        <div class="final-result" id="tagging-final-result" style="display:none;"></div>
    `;
    document.getElementById('tagging-actions').style.display = 'none';
    
    const stepsEl = document.getElementById('tagging-steps');
    const thinkingEl = document.getElementById('tagging-thinking-content');
    const finalResultEl = document.getElementById('tagging-final-result');
    
    try {
        // 使用 SSE 流式请求
        const response = await fetch('/api/agent/tagging/generate/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ table_name: tableName })
        });
        
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop();
            
            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    try {
                        const event = JSON.parse(line.slice(6));
                        handleTaggingStreamEvent(event, stepsEl, thinkingEl, finalResultEl, tableName);
                    } catch (e) {
                        console.error('解析事件失败:', e);
                    }
                }
            }
        }
        
        showToast('标签生成完成', 'success');
        
    } catch (error) {
        showToast('生成失败: ' + error.message, 'error');
        thinkingEl.innerHTML += `<div class="error-message">❌ 错误: ${error.message}</div>`;
    }
}

/**
 * 处理标签生成流式事件
 */
function handleTaggingStreamEvent(event, stepsEl, thinkingEl, finalResultEl, tableName) {
    switch (event.type) {
        case 'start':
            thinkingEl.innerHTML += `<div class="thinking-item">🚀 ${event.message}</div>`;
            break;
        
        case 'step':
            updateStepStatus(stepsEl, event);
            break;
            
        case 'tool_result':
            thinkingEl.innerHTML += `
                <div class="tool-result">
                    <div class="tool-name">📦 工具调用: ${event.tool}</div>
                    <pre class="tool-output">${escapeHtml(event.result.substring(0, 500))}${event.result.length > 500 ? '...' : ''}</pre>
                </div>
            `;
            thinkingEl.scrollTop = thinkingEl.scrollHeight;
            break;
            
        case 'thinking':
            thinkingEl.innerHTML += `<div class="thinking-item">💭 ${event.message}</div>`;
            break;
            
        case 'result':
            state.generatedTags = { ...event.data, table_name: tableName };
            finalResultEl.style.display = 'block';
            
            const tableTags = event.data.table_tags || [];
            const columnTags = event.data.column_tags || {};
            
            let tagsHtml = `
                <h4>✅ 生成的标签</h4>
                <div class="result-section">
                    <h5>表标签</h5>
                    <div class="tag-list">
                        ${tableTags.map(tag => `<span class="tag tag-llm-preview">${tag}</span>`).join('')}
                    </div>
                </div>
                <div class="result-section">
                    <h5>字段标签</h5>
            `;
            
            for (const [col, tags] of Object.entries(columnTags)) {
                tagsHtml += `
                    <div class="column-tags-preview">
                        <span class="column-name">${col}</span>
                        <div class="tag-list">
                            ${tags.map(tag => `<span class="tag tag-llm-preview">${tag}</span>`).join('')}
                        </div>
                    </div>
                `;
            }
            
            tagsHtml += '</div>';
            finalResultEl.innerHTML = tagsHtml;
            document.getElementById('tagging-actions').style.display = 'flex';
            break;
            
        case 'error':
            thinkingEl.innerHTML += `<div class="error-message">❌ 错误: ${event.message}</div>`;
            break;
            
        case 'end':
            thinkingEl.innerHTML += `<div class="thinking-item">✨ ${event.message}</div>`;
            break;
    }
}

/**
 * 应用标签到数据库
 */
async function applyTags() {
    if (!state.generatedTags) return;
    
    showLoading('正在应用标签...');
    
    try {
        await fetchAPI('/api/agent/tagging/apply', {
            method: 'POST',
            body: JSON.stringify({
                table_name: state.generatedTags.table_name,
                table_tags: state.generatedTags.table_tags || [],
                column_tags: state.generatedTags.column_tags || {}
            })
        });
        
        showToast('标签已成功应用到数据库', 'success');
        
        // 刷新现有标签显示
        await loadExistingTags(state.generatedTags.table_name);
        
    } catch (error) {
        showToast('应用失败: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

// ============ SQL纠错校验Agent模块 ============

/**
 * 生成随机SQL（用于测试）
 */
async function generateRandomSQL() {
    showLoading('正在生成测试SQL...');
    
    // 显示模型调用过程区
    const processBox = document.getElementById('model-process-box');
    const processContent = document.getElementById('model-process-content');
    processBox.style.display = 'block';
    processContent.innerHTML = '';
    
    // 添加调用开始记录
    addProcessItem(processContent, 'start', '开始调用', '请求 API: /api/agent/sql-validation/generate');
    
    try {
        const startTime = Date.now();
        const result = await fetchAPI('/api/agent/sql-validation/generate', {
            method: 'POST',
            body: JSON.stringify({})
        });
        const endTime = Date.now();
        
        // 添加API响应记录
        const sqlType = result.sql_type || result.type || '未知';
        addProcessItem(processContent, 'api', 'API 响应', `耗时: ${endTime - startTime}ms | 生成SQL类型: ${sqlType}`);
        
        // 注意：现在是 contenteditable 的 pre 标签，不是 textarea
        const sqlDisplay = document.getElementById('sql-to-validate');
        sqlDisplay.textContent = result.sql;
        
        // 显示SQL类型提示
        const hintEl = document.getElementById('sql-type-hint');
        if (hintEl) {
            const hintTextEl = hintEl.querySelector('.meta-text');
            if (hintTextEl) {
                hintEl.style.display = 'inline-flex';
                
                let hintText = '正常SQL';
                const actualType = result.sql_type || result.type;
                
                if (actualType === 'syntax_error') {
                    hintText = '此SQL包含语法错误（用于测试）';
                } else if (actualType === 'performance_issue') {
                    hintText = '此SQL可能存在性能问题（用于测试）';
                } else {
                    hintText = '正常SQL（用于测试）';
                }
                
                hintTextEl.textContent = hintText;
            }
        }
        
        // 添加完成记录
        addProcessItem(processContent, 'success', '生成成功', `SQL长度: ${result.sql ? result.sql.length : 0} 字符`);
        
        state.generatedTestSQL = result;
        showToast('测试SQL已生成', 'success');
        
    } catch (error) {
        // 添加错误记录
        addProcessItem(processContent, 'error', '生成失败', error.message);
        showToast('生成失败: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

/**
 * 添加模型调用过程记录
 */
function addProcessItem(container, type, title, content) {
    const time = new Date().toLocaleTimeString('zh-CN', { hour12: false });
    const iconMap = {
        'start': '🚀',
        'api': '🌐',
        'model': '🤖',
        'tool': '🔧',
        'success': '✅',
        'error': '❌'
    };
    
    const item = document.createElement('div');
    item.className = 'process-item';
    item.innerHTML = `
        <div class="process-item-header">
            <div class="process-item-type">
                <span class="process-item-icon">${iconMap[type] || '📌'}</span>
                <span>${title}</span>
            </div>
            <span class="process-item-time">${time}</span>
        </div>
        <div class="process-item-body">${content}</div>
    `;
    
    container.appendChild(item);
    // 自动滚动到底部
    container.scrollTop = container.scrollHeight;
}

/**
 * 校验SQL（流式输出）
 */
async function validateSQL() {
    // 注意：现在是contenteditable的pre标签，不是textarea
    const sqlDisplay = document.getElementById('sql-to-validate');
    const sql = sqlDisplay.textContent.trim();
    
    if (!sql || sql === '点击"生成SQL"按钮随机生成测试SQL\n或直接在此编辑输入您的SQL语句...') {
        showToast('请输入要校验的SQL', 'error');
        return;
    }
    
    // 显示模型调用过程区
    const processBox = document.getElementById('model-process-box');
    const processContent = document.getElementById('model-process-content');
    processBox.style.display = 'block';
    processContent.innerHTML = '';
    
    // 添加校验开始记录
    addProcessItem(processContent, 'start', '开始校验', `SQL长度: ${sql.length} 字符`);
    
    // 显示校验过程区（中间区域）
    const validationProcessBox = document.getElementById('validation-process-box');
    validationProcessBox.style.display = 'block';
    const stepsEl = document.getElementById('validation-steps');
    const thinkingEl = document.getElementById('validation-thinking-content');
    stepsEl.innerHTML = '';
    thinkingEl.innerHTML = '';
    
    // 清空结果区
    const resultEl = document.getElementById('validation-result');
    const suggestionsEl = document.getElementById('validation-suggestions');
    
    resultEl.innerHTML = `
        <div class="placeholder-content">
            <span class="placeholder-icon">🕒</span>
            <p class="placeholder-text">正在分析...</p>
        </div>
    `;
    
    suggestionsEl.innerHTML = `
        <div class="placeholder-content">
            <span class="placeholder-icon">🕒</span>
            <p class="placeholder-text">正在分析...</p>
        </div>
    `;
    
    document.getElementById('sql-diff-panel').style.display = 'none';
    
    const finalResultEl = resultEl;
    
    try {
        // 使用 SSE 流式请求
        const response = await fetch('/api/agent/sql-validation/validate/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql: sql })
        });
        
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop();
            
            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    try {
                        const event = JSON.parse(line.slice(6));
                        handleValidationStreamEvent(event, stepsEl, thinkingEl, finalResultEl, sql);
                    } catch (e) {
                        console.error('解析事件失败:', e);
                    }
                }
            }
        }
        
        showToast('SQL校验完成', 'success');
        
    } catch (error) {
        showToast('校验失败: ' + error.message, 'error');
        thinkingEl.innerHTML += `<div class="error-message">❌ 错误: ${error.message}</div>`;
    }
}

/**
 * 处理SQL校验流式事件
 */
function handleValidationStreamEvent(event, stepsEl, thinkingEl, finalResultEl, originalSQL) {
    const processContent = document.getElementById('model-process-content');
    
    switch (event.type) {
        case 'start':
            thinkingEl.innerHTML += `<div class="thinking-item">🚀 ${event.message}</div>`;
            if (processContent) {
                addProcessItem(processContent, 'api', 'API 响应', event.message);
            }
            break;
        
        case 'step':
            updateStepStatus(stepsEl, event);
            if (processContent && event.title) {
                addProcessItem(processContent, 'tool', '步骤更新', event.title + (event.message ? ': ' + event.message : ''));
            }
            break;
            
        case 'tool_result':
            thinkingEl.innerHTML += `
                <div class="tool-result">
                    <div class="tool-name">📦 工具调用: ${event.tool}</div>
                    <pre class="tool-output">${escapeHtml(event.result.substring(0, 500))}${event.result.length > 500 ? '...' : ''}</pre>
                </div>
            `;
            thinkingEl.scrollTop = thinkingEl.scrollHeight;
            if (processContent) {
                addProcessItem(processContent, 'tool', `调用工具: ${event.tool}`, `返回结果长度: ${event.result ? event.result.length : 0} 字符`);
            }
            break;
            
        case 'thinking':
            thinkingEl.innerHTML += `<div class="thinking-item">💭 ${event.message}</div>`;
            if (processContent) {
                addProcessItem(processContent, 'model', 'Agent 思考', event.message);
            }
            break;
            
        case 'result':
            state.validationResult = event.data;
            finalResultEl.style.display = 'block';
            
            const isValid = event.data.is_valid;
            const errors = event.data.errors || [];
            const warnings = event.data.warnings || [];
            const fixedSQL = event.data.fixed_sql;
            
            const suggestionsEl = document.getElementById('validation-suggestions');
            
            let resultHtml = '';
            let suggestionsHtml = '';
            
            if (isValid && errors.length === 0 && warnings.length === 0) {
                resultHtml = `
                    <div class="validation-success">
                        <h4>✅ SQL校验通过</h4>
                        <p>该SQL语法正确，未发现明显性能问题。</p>
                    </div>
                `;
                suggestionsHtml = `
                    <div class="placeholder-content">
                        <span class="placeholder-icon">✨</span>
                        <p class="placeholder-text">SQL完美，无需修改建议</p>
                    </div>
                `;
            } else {
                // 校验结果区：显示错误和警告
                resultHtml = '<div class="validation-issues">';
                
                if (errors.length > 0) {
                    resultHtml += `
                        <div class="error-section">
                            <h4>❌ 语法错误 (${errors.length})</h4>
                            <ul class="error-list">
                                ${errors.map(e => {
                                    const msg = typeof e === 'object' ? (e.message || JSON.stringify(e)) : e;
                                    return `<li class="validation-error">${escapeHtml(msg)}</li>`;
                                }).join('')}
                            </ul>
                        </div>
                    `;
                }
                
                if (warnings.length > 0) {
                    resultHtml += `
                        <div class="warning-section">
                            <h4>⚠️ 性能警告 (${warnings.length})</h4>
                            <ul class="warning-list">
                                ${warnings.map(w => {
                                    const msg = typeof w === 'object' ? (w.message || JSON.stringify(w)) : w;
                                    return `<li class="validation-warning">${escapeHtml(msg)}</li>`;
                                }).join('')}
                            </ul>
                        </div>
                    `;
                }
                
                resultHtml += '</div>';
                
                // 修改建议区：显示修复后的SQL + 左右对比
                if (fixedSQL && fixedSQL !== originalSQL) {
                    suggestionsHtml = `
                        <div class="suggestion-content">
                            <div class="suggestion-header">
                                <span class="suggestion-icon">🤖</span>
                                <span class="suggestion-title">AI 推荐的修复方案</span>
                            </div>
                            <div class="suggestion-text">
                                <p>检测到问题后，AI 已为您生成了修复后的 SQL，请查看下方对比。</p>
                            </div>
                            
                            <!-- 修复后SQL展示 -->
                            <div class="fixed-sql-display">
                                <div class="fixed-sql-header">
                                    <span>✅</span>
                                    <span>修复后SQL</span>
                                </div>
                                <pre class="fixed-sql-code">${escapeHtml(fixedSQL)}</pre>
                            </div>
                            
                            <div class="suggestion-actions">
                                <button class="btn btn-primary" onclick="applyFixedSQL()">
                                    <span class="btn-icon">✔️</span>
                                    应用修复后SQL
                                </button>
                                <button class="btn btn-secondary" onclick="copyFixedSQL()">
                                    <span class="btn-icon">📋</span>
                                    复制修复后SQL
                                </button>
                            </div>
                        </div>
                    `;
                } else {
                    suggestionsHtml = `
                        <div class="placeholder-content">
                            <span class="placeholder-icon">💡</span>
                            <p class="placeholder-text">暂无自动修复建议，请根据上方错误提示手动修改</p>
                        </div>
                    `;
                }
            }
            
            finalResultEl.innerHTML = resultHtml;
            suggestionsEl.innerHTML = suggestionsHtml;
            
            // 添加校验完成记录
            if (processContent) {
                if (isValid && errors.length === 0 && warnings.length === 0) {
                    addProcessItem(processContent, 'success', '校验完成', 'SQL语法正确，无性能问题');
                } else {
                    const issueCount = errors.length + warnings.length;
                    addProcessItem(processContent, 'success', '校验完成', `发现 ${issueCount} 个问题 (错误: ${errors.length}, 警告: ${warnings.length})`);
                }
            }
            
            // 如果有修复后的SQL，显示diff对比
            if (fixedSQL && fixedSQL !== originalSQL) {
                document.getElementById('sql-diff-panel').style.display = 'block';
                renderSQLDiff(originalSQL, fixedSQL);
            }
            break;
            
        case 'error':
            thinkingEl.innerHTML += `<div class="error-message">❌ 错误: ${event.message}</div>`;
            if (processContent) {
                addProcessItem(processContent, 'error', '执行错误', event.message);
            }
            break;
            
        case 'end':
            thinkingEl.innerHTML += `<div class="thinking-item">✨ ${event.message}</div>`;
            break;
    }
}

/**
 * 渲染SQL差异对比（左右全宽展示）
 */
function renderSQLDiff(originalSQL, fixedSQL) {
    const diffContainer = document.getElementById('sql-diff');
    
    // 左右对比布局
    let diffHtml = `
        <div class="diff-side-by-side">
            <div class="diff-column original">
                <div class="diff-column-header">原始SQL</div>
                <div class="diff-column-body">
                    <pre class="diff-sql-code">${escapeHtml(originalSQL)}</pre>
                </div>
            </div>
            <div class="diff-column fixed">
                <div class="diff-column-header">修复后SQL</div>
                <div class="diff-column-body">
                    <pre class="diff-sql-code">${escapeHtml(fixedSQL)}</pre>
                </div>
            </div>
        </div>
    `;
    
    diffContainer.innerHTML = diffHtml;
}

/**
 * 应用修复后的SQL
 */
function applyFixedSQL() {
    if (state.validationResult && state.validationResult.fixed_sql) {
        // 注意：现在是contenteditable的pre标签
        const sqlDisplay = document.getElementById('sql-to-validate');
        sqlDisplay.textContent = state.validationResult.fixed_sql;
        document.getElementById('sql-type-hint').style.display = 'none';
        showToast('已应用修复后的SQL', 'success');
    }
}

/**
 * 复制修复后的SQL
 */
function copyFixedSQL() {
    if (state.validationResult && state.validationResult.fixed_sql) {
        navigator.clipboard.writeText(state.validationResult.fixed_sql).then(() => {
            showToast('修复后SQL已复制到剪贴板', 'success');
        });
    }
}

// ============ 事件绑定 ============

document.addEventListener('DOMContentLoaded', () => {
    // 加载初始数据
    loadDatabaseSummary();
    loadTableList();
    loadMissingMetadata();
    loadERDiagram();
    
    // 导航标签切换
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.dataset.tab;
            
            // 更新按钮状态
            document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            // 切换内容区
            document.querySelectorAll('.tab-section').forEach(s => s.classList.remove('active'));
            document.getElementById(tab + '-section').classList.add('active');
        });
    });
    
    // 子标签切换
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const subtab = btn.dataset.subtab;
            const parent = btn.closest('.panel');
            
            parent.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            parent.querySelectorAll('.subtab-content').forEach(s => s.classList.remove('active'));
            parent.querySelector(`#${subtab}-subtab`).classList.add('active');
        });
    });
    
    // 表搜索
    document.getElementById('table-search').addEventListener('input', (e) => {
        const keyword = e.target.value.toLowerCase();
        const filtered = state.tables.filter(t => 
            t.table_name.toLowerCase().includes(keyword) ||
            (t.description && t.description.toLowerCase().includes(keyword))
        );
        renderTableList(filtered);
    });
    
    // 分页按钮
    document.getElementById('prev-page').addEventListener('click', () => {
        if (state.currentPage > 0 && state.currentTable) {
            loadTableData(state.currentTable, state.currentPage - 1);
        }
    });
    
    document.getElementById('next-page').addEventListener('click', () => {
        if (state.currentTable && (state.currentPage + 1) * state.pageSize < state.totalRows) {
            loadTableData(state.currentTable, state.currentPage + 1);
        }
    });
    
    // 元数据Agent按钮
    document.getElementById('generate-metadata-btn').addEventListener('click', generateMetadata);
    document.getElementById('apply-metadata-btn').addEventListener('click', applyMetadata);
    document.getElementById('copy-metadata-btn').addEventListener('click', copyMetadata);
    
    // SQL Agent按钮
    document.getElementById('generate-sql-btn').addEventListener('click', generateSQL);
    document.getElementById('execute-sql-btn').addEventListener('click', executeSQL);
    document.getElementById('copy-sql-btn').addEventListener('click', copySQL);
    document.getElementById('explain-sql-btn').addEventListener('click', explainSQL);
    
    // 数据资产打标Agent按钮
    const taggingTableSelect = document.getElementById('tagging-table-select');
    if (taggingTableSelect) {
        taggingTableSelect.addEventListener('change', (e) => {
            loadExistingTags(e.target.value);
        });
    }
    
    const generateTagsBtn = document.getElementById('generate-tags-btn');
    if (generateTagsBtn) {
        generateTagsBtn.addEventListener('click', generateTags);
    }
    
    const applyTagsBtn = document.getElementById('apply-tags-btn');
    if (applyTagsBtn) {
        applyTagsBtn.addEventListener('click', applyTags);
    }
    
    // SQL校验Agent按钮
    const generateRandomSqlBtn = document.getElementById('generate-random-sql');
    if (generateRandomSqlBtn) {
        generateRandomSqlBtn.addEventListener('click', generateRandomSQL);
    }
    
    const validateSqlBtn = document.getElementById('validate-sql-btn');
    if (validateSqlBtn) {
        validateSqlBtn.addEventListener('click', validateSQL);
    }
    
    // 示例查询链接
    document.querySelectorAll('.example-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            document.getElementById('sql-requirement').value = link.dataset.query;
        });
    });
    
    // 弹窗关闭
    document.querySelector('.modal-close').addEventListener('click', hideModal);
    document.getElementById('modal-cancel').addEventListener('click', hideModal);
    document.getElementById('modal').addEventListener('click', (e) => {
        if (e.target === document.getElementById('modal')) {
            hideModal();
        }
    });
});
