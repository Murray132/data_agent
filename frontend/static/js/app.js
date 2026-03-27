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
const LOCAL_DATASOURCE_ID = 'local_finance';
const LOCAL_DATASOURCE = {
    id: LOCAL_DATASOURCE_ID,
    name: '本地数据源',
    type: 'sqlite',
    engine: 'sqlite',
    database: 'data/finance.db',
    is_builtin: true,
    status: 'online',
    description: '系统内置本地数据源'
};

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
    generatedTestSQL: null,
    // 模型配置状态
    modelConfigLoaded: false,
    currentModelConfig: null,
    // 数据源状态
    dataSources: [LOCAL_DATASOURCE],
    selectedDataSourceId: LOCAL_DATASOURCE_ID,
    // 认证状态
    currentUser: null,
    permissions: [],
    appInitialized: false
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
        if (response.status === 401) {
            showAuthOverlay();
        }
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
    const modalEl = document.getElementById('modal');
    modalEl.classList.remove('show');
    modalEl.classList.remove('modal-explain');
}

function hasPermission(permission) {
    return state.permissions.includes(permission);
}

function getAvailableDataSources() {
    return (state.dataSources && state.dataSources.length > 0) ? state.dataSources : [LOCAL_DATASOURCE];
}

function getSelectedDataSource() {
    const source = getAvailableDataSources().find((item) => item.id === state.selectedDataSourceId);
    return source || LOCAL_DATASOURCE;
}

function isLocalDataSourceSelected() {
    return getSelectedDataSource().id === LOCAL_DATASOURCE_ID;
}

function buildDataSourceScopedUrl(path) {
    const url = new URL(path, window.location.origin);
    url.searchParams.set('datasource_id', getSelectedDataSource().id);
    return `${url.pathname}${url.search}`;
}

function updateCurrentDataSourceBanner() {
    const bannerEl = document.getElementById('current-datasource-banner');
    if (!bannerEl) return;
    const source = getSelectedDataSource();
    const readonlyText = isLocalDataSourceSelected() ? '' : '，当前为只读浏览';
    bannerEl.textContent = `当前数据源：${source.name} (${String(source.type || '').toUpperCase()})${readonlyText}`;
}

function getDefaultTemperature() {
    if (state.currentModelConfig && typeof state.currentModelConfig.temperature === 'number') {
        return state.currentModelConfig.temperature;
    }
    const input = document.getElementById('model-temperature');
    const value = Number(input?.value || 0.7);
    return Number.isNaN(value) ? 0.7 : value;
}

function syncInferenceControlsWithConfig(config) {
    const temperatureValue = config && typeof config.temperature === 'number' ? config.temperature : 0.7;

    const sqlTemp = document.getElementById('sql-temperature');
    const validationTemp = document.getElementById('validation-temperature');

    if (sqlTemp) sqlTemp.value = String(temperatureValue);
    if (validationTemp) validationTemp.value = String(temperatureValue);
}

function getSQLInferenceOptions() {
    const tempEl = document.getElementById('sql-temperature');
    const temperature = Number(tempEl?.value || getDefaultTemperature());
    return {
        temperature: Number.isNaN(temperature) ? getDefaultTemperature() : temperature
    };
}

function getValidationInferenceOptions() {
    const tempEl = document.getElementById('validation-temperature');
    const temperature = Number(tempEl?.value || getDefaultTemperature());
    return {
        temperature: Number.isNaN(temperature) ? getDefaultTemperature() : temperature
    };
}

function getVisibleTabButtons() {
    return Array.from(document.querySelectorAll('.nav-btn[data-tab]')).filter((btn) => {
        if (btn.style.display === 'none') return false;
        return window.getComputedStyle(btn).display !== 'none';
    });
}

function showAuthOverlay(message = '') {
    const overlay = document.getElementById('auth-overlay');
    if (overlay) overlay.classList.add('show');
    if (message) {
        const errorEl = document.getElementById('auth-error');
        if (errorEl) errorEl.textContent = message;
    }
}

function hideAuthOverlay() {
    const overlay = document.getElementById('auth-overlay');
    if (overlay) overlay.classList.remove('show');
    const errorEl = document.getElementById('auth-error');
    if (errorEl) errorEl.textContent = '';
}

function applyPermissionUI() {
    const navPermissionMap = {
        'tables': 'tables.read',
        'datasources': 'datasource.manage',
        'metadata-agent': 'agent.metadata',
        'sql-agent': 'agent.sql',
        'tagging-agent': 'agent.tagging',
        'sql-validation': 'agent.validation',
        'model-config': 'model_config.manage'
    };

    document.querySelectorAll('.nav-btn[data-tab]').forEach(btn => {
        const tab = btn.dataset.tab;
        const permission = navPermissionMap[tab];
        if (!permission) return;
        const allowed = hasPermission(permission);
        btn.style.display = allowed ? '' : 'none';
    });

    const buttonPermissionMap = {
        'apply-metadata-btn': 'tables.write',
        'generate-tags-btn': 'agent.tagging',
        'apply-tags-btn': 'agent.tagging',
        'validate-sql-btn': 'agent.validation',
        'generate-random-sql': 'agent.validation',
        'execute-sql-btn': 'sql.execute',
        'load-model-config-btn': 'model_config.manage',
        'test-model-config-btn': 'model_config.manage',
        'save-model-config-btn': 'model_config.manage',
        'refresh-datasources-btn': 'datasource.manage',
        'test-datasource-btn': 'datasource.manage',
        'save-datasource-btn': 'datasource.manage'
    };

    Object.entries(buttonPermissionMap).forEach(([id, permission]) => {
        const el = document.getElementById(id);
        if (!el) return;
        const allowed = hasPermission(permission);
        el.style.display = allowed ? '' : 'none';
    });

    // 如果当前激活标签被权限隐藏，自动切换到第一个可见标签
    const activeBtn = document.querySelector('.nav-btn.active[data-tab]');
    const firstVisibleBtn = getVisibleTabButtons()[0];
    if (!activeBtn && firstVisibleBtn) {
        switchMainTab(firstVisibleBtn.dataset.tab);
        return;
    }
    if (activeBtn && activeBtn.style.display === 'none' && firstVisibleBtn) {
        switchMainTab(firstVisibleBtn.dataset.tab);
    }
}

function switchMainTab(tab) {
    if (!tab) return;

    const targetBtn = document.querySelector(`.nav-btn[data-tab="${tab}"]`);
    const targetSection = document.getElementById(`${tab}-section`);
    if (!targetBtn || !targetSection) {
        console.error('[TabSwitch] 无法切换标签:', { tab, targetBtn, targetSection });
        showToast(`页面切换失败: 未找到 ${tab} 对应区域`, 'error');
        return;
    }

    document.querySelectorAll('.nav-btn[data-tab]').forEach(b => b.classList.remove('active'));
    targetBtn.classList.add('active');

    document.querySelectorAll('.tab-section').forEach(s => s.classList.remove('active'));
    targetSection.classList.add('active');

    if (tab === 'model-config' && !state.modelConfigLoaded) {
        loadModelConfig();
    }
}

function updateUserPanel() {
    const panel = document.getElementById('user-panel');
    const label = document.getElementById('current-user-label');
    if (!panel || !label) return;
    if (!state.currentUser) {
        panel.style.display = 'none';
        return;
    }

    panel.style.display = 'flex';
    label.textContent = `${state.currentUser.display_name} (${state.currentUser.role})`;
}

function initializeAppData() {
    if (state.appInitialized) return;
    state.appInitialized = true;

    loadDatabaseSummary();
    loadTableList();
    if (hasPermission('agent.metadata')) {
        loadMissingMetadata();
    }
    loadERDiagram();
    if (hasPermission('datasource.manage')) {
        loadDataSources(false);
    }
    if (hasPermission('model_config.manage')) {
        loadModelConfig(false);
    }
}

async function checkAuthSession() {
    try {
        const data = await fetchAPI('/api/auth/me');
        state.currentUser = data.user;
        state.permissions = data.user.permissions || [];
        updateUserPanel();
        applyPermissionUI();
        hideAuthOverlay();
        return true;
    } catch (error) {
        state.currentUser = null;
        state.permissions = [];
        updateUserPanel();
        showAuthOverlay('请先登录');
        return false;
    }
}

async function login() {
    const usernameEl = document.getElementById('auth-username');
    const passwordEl = document.getElementById('auth-password');
    const errorEl = document.getElementById('auth-error');
    const username = usernameEl ? usernameEl.value.trim() : '';
    const password = passwordEl ? passwordEl.value : '';

    if (!username || !password) {
        if (errorEl) errorEl.textContent = '请输入用户名和密码';
        return;
    }

    try {
        const result = await fetchAPI('/api/auth/login', {
            method: 'POST',
            body: JSON.stringify({ username, password })
        });
        state.currentUser = result.user;
        state.permissions = result.user.permissions || [];
        updateUserPanel();
        applyPermissionUI();
        hideAuthOverlay();
        initializeAppData();
        showToast('登录成功', 'success');
    } catch (error) {
        if (errorEl) errorEl.textContent = error.message;
    }
}

async function logout() {
    try {
        await fetchAPI('/api/auth/logout', { method: 'POST' });
    } catch (error) {
        console.warn('退出登录请求失败:', error.message);
    } finally {
        state.currentUser = null;
        state.permissions = [];
        updateUserPanel();
        showAuthOverlay('已退出登录');
    }
}

/**
 * 更新模型配置状态提示
 */
function updateModelConfigStatus(message, type = 'info') {
    const statusEl = document.getElementById('model-config-status');
    if (!statusEl) return;

    const colorMap = {
        info: { bg: '#f7f9fc', border: '#dbe3ef', color: '#4b5563' },
        success: { bg: '#f0fdf4', border: '#86efac', color: '#166534' },
        error: { bg: '#fef2f2', border: '#fca5a5', color: '#b91c1c' }
    };
    const style = colorMap[type] || colorMap.info;
    statusEl.textContent = message;
    statusEl.style.background = style.bg;
    statusEl.style.borderColor = style.border;
    statusEl.style.color = style.color;
}

/**
 * 读取模型配置表单
 */
function getModelConfigFormValues() {
    return {
        base_url: document.getElementById('model-base-url').value.trim(),
        api_key: document.getElementById('model-api-key').value.trim(),
        model_name: document.getElementById('model-name').value.trim(),
        temperature: Number(document.getElementById('model-temperature')?.value || 0.7)
    };
}

/**
 * 推断模型供应商名称
 */
function inferModelProvider(baseUrl) {
    if (!baseUrl) return '未知';
    const value = baseUrl.toLowerCase();
    if (value.includes('openai.com')) return 'OpenAI';
    if (value.includes('anthropic.com')) return 'Anthropic';
    if (value.includes('aliyuncs.com')) return 'OpenAI-Compatible Endpoint';
    if (value.includes('deepseek.com')) return 'DeepSeek';
    if (value.includes('volces.com') || value.includes('ark.cn-beijing')) return '火山方舟';
    return '兼容 OpenAI 的自定义供应商';
}

/**
 * 渲染当前模型信息（折叠框）
 */
function renderCurrentModelInfo(config) {
    const infoEl = document.getElementById('model-current-info');
    if (!infoEl) return;

    const provider = inferModelProvider(config.base_url);
    infoEl.innerHTML = `
        <div class="model-current-line">供应商：${provider}</div>
        <div class="model-current-line">模型：${config.model_name || '未配置'}</div>
        <div class="model-current-line">Base URL：${config.base_url || '未配置'}</div>
        <div class="model-current-line">temperature：${config.temperature ?? 0.7}</div>
    `;
}

/**
 * 回填模型配置表单
 */
function setModelConfigFormValues(config) {
    document.getElementById('model-base-url').value = config.base_url || '';
    document.getElementById('model-api-key').value = '';
    document.getElementById('model-name').value = config.model_name || '';
    const temperatureInput = document.getElementById('model-temperature');
    if (temperatureInput) {
        temperatureInput.value = String(config.temperature ?? 0.7);
    }
    syncInferenceControlsWithConfig(config);
    renderCurrentModelInfo(config);
}

/**
 * 校验模型配置输入
 */
function validateModelConfigInput(configData) {
    if (!configData.base_url) {
        throw new Error('Base URL 不能为空');
    }
    if (!configData.model_name) {
        throw new Error('Model Name 不能为空');
    }
    if (Number.isNaN(Number(configData.temperature))) {
        throw new Error('temperature 不合法');
    }
    if (!configData.api_key) {
        throw new Error('API Key 不能为空，请输入或先加载已有配置');
    }
}

/**
 * 构造模型配置请求参数（API Key 可沿用已有配置）
 */
function buildModelConfigPayload() {
    const formData = getModelConfigFormValues();
    const fallbackKey = state.currentModelConfig ? (state.currentModelConfig.full_key || '') : '';

    return {
        base_url: formData.base_url,
        model_name: formData.model_name,
        api_key: formData.api_key || fallbackKey,
        temperature: formData.temperature
    };
}

/**
 * 加载当前模型配置
 */
async function loadModelConfig(withLoading = true) {
    if (withLoading) showLoading('正在加载模型配置...');

    try {
        const configData = await fetchAPI('/api/model/config');
        state.currentModelConfig = configData;
        setModelConfigFormValues(configData);
        state.modelConfigLoaded = true;
        updateModelConfigStatus('已加载当前模型配置', 'success');
    } catch (error) {
        updateModelConfigStatus('加载模型配置失败: ' + error.message, 'error');
        showToast('加载模型配置失败: ' + error.message, 'error');
    } finally {
        if (withLoading) hideLoading();
    }
}

/**
 * 测试模型配置可用性
 */
async function testModelConfig() {
    try {
        const payload = buildModelConfigPayload();
        validateModelConfigInput(payload);

        showLoading('正在测试模型连接...');
        const result = await fetchAPI('/api/model/test', {
            method: 'POST',
            body: JSON.stringify(payload)
        });

        const shortResponse = result.response ? ` | 返回: ${String(result.response).slice(0, 80)}` : '';
        updateModelConfigStatus(`测试成功: ${result.message || '连接可用'}${shortResponse}`, 'success');
        showToast('模型配置测试成功', 'success');
    } catch (error) {
        updateModelConfigStatus('测试失败: ' + error.message, 'error');
        showToast('模型配置测试失败: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

/**
 * 保存模型配置
 */
async function saveModelConfig() {
    try {
        const payload = buildModelConfigPayload();
        validateModelConfigInput(payload);

        showLoading('正在保存模型配置...');
        const result = await fetchAPI('/api/model/config', {
            method: 'POST',
            body: JSON.stringify(payload)
        });

        state.currentModelConfig = {
            base_url: payload.base_url,
            model_name: payload.model_name,
            full_key: payload.api_key,
            temperature: payload.temperature
        };
        renderCurrentModelInfo(state.currentModelConfig);
        document.getElementById('model-api-key').value = '';

        updateModelConfigStatus('保存成功，后续 Agent 调用将使用新配置', 'success');
        showToast('模型配置已保存', 'success');
        state.modelConfigLoaded = true;
    } catch (error) {
        updateModelConfigStatus('保存失败: ' + error.message, 'error');
        showToast('保存模型配置失败: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

/**
 * 切换 API Key 显示状态
 */
function toggleApiKeyVisibility() {
    const inputEl = document.getElementById('model-api-key');
    const toggleBtn = document.getElementById('toggle-api-key-visibility');
    if (!inputEl || !toggleBtn) return;

    const isPassword = inputEl.type === 'password';
    inputEl.type = isPassword ? 'text' : 'password';
    toggleBtn.textContent = isPassword ? '隐藏' : '显示';
}

// ============ 数据源管理模块 ============

function updateDataSourceStatus(message, type = 'info') {
    const statusEl = document.getElementById('datasource-status');
    if (!statusEl) return;
    const colorMap = {
        info: { bg: '#f7f9fc', border: '#dbe3ef', color: '#4b5563' },
        success: { bg: '#f0fdf4', border: '#86efac', color: '#166534' },
        error: { bg: '#fef2f2', border: '#fca5a5', color: '#b91c1c' }
    };
    const style = colorMap[type] || colorMap.info;
    statusEl.textContent = message;
    statusEl.style.background = style.bg;
    statusEl.style.borderColor = style.border;
    statusEl.style.color = style.color;
}

function getDataSourceFormValues() {
    return {
        name: (document.getElementById('ds-name')?.value || '').trim(),
        type: (document.getElementById('ds-type')?.value || 'mysql').trim(),
        host: (document.getElementById('ds-host')?.value || '').trim(),
        port: Number(document.getElementById('ds-port')?.value || 3306),
        username: (document.getElementById('ds-username')?.value || '').trim(),
        password: (document.getElementById('ds-password')?.value || '').trim(),
        database: (document.getElementById('ds-database')?.value || '').trim(),
        charset: (document.getElementById('ds-charset')?.value || 'utf8mb4').trim()
    };
}

function validateDataSourceForm(payload) {
    if (!payload.name) throw new Error('数据源名称不能为空');
    if (!payload.host) throw new Error('Host 不能为空');
    if (!payload.port || Number.isNaN(payload.port)) throw new Error('Port 不合法');
    if (!payload.username) throw new Error('Username 不能为空');
    if (!payload.password) throw new Error('Password 不能为空');
    if (!payload.database) throw new Error('Database 不能为空');
}

function renderDataSourceList(sources) {
    const listEl = document.getElementById('datasource-list');
    if (!listEl) return;
    if (!sources || sources.length === 0) {
        listEl.innerHTML = '<div class="placeholder">暂无数据源</div>';
        return;
    }

    listEl.innerHTML = sources.map((s) => {
        const builtIn = s.is_builtin ? '<span class="pk-badge">内置</span>' : '<span class="notnull-badge">外部</span>';
        const active = s.id === getSelectedDataSource().id ? '<span class="pk-badge">当前使用</span>' : '';
        const endpoint = s.type === 'sqlite'
            ? (s.database || '-')
            : `${s.host || '-'}:${s.port || '-'} / ${s.database || '-'}`;
        const actions = `
            <div class="datasource-actions">
                <button class="btn btn-small btn-primary" data-action="use" data-id="${s.id}" type="button">使用</button>
                ${s.is_builtin ? '' : `
                <button class="btn btn-small btn-info" data-action="test" data-id="${s.id}" type="button">测试</button>
                <button class="btn btn-small btn-secondary" data-action="delete" data-id="${s.id}" type="button">删除</button>
                `}
            </div>`;
        return `
            <div class="datasource-item">
                <div class="datasource-head">
                    <div class="name">${escapeHtml(s.name || '未命名数据源')}</div>
                    <div>${builtIn}${active}</div>
                </div>
                <div class="datasource-meta">类型：${escapeHtml((s.type || '').toUpperCase())}</div>
                <div class="datasource-meta">连接：${escapeHtml(endpoint)}</div>
                ${actions}
            </div>
        `;
    }).join('');
}

async function loadDataSources(withLoading = true) {
    if (withLoading) showLoading('正在加载数据源...');
    try {
        const data = await fetchAPI('/api/datasources');
        const sources = data.sources || [];
        state.dataSources = sources.length > 0 ? sources : [LOCAL_DATASOURCE];
        if (!state.dataSources.some((item) => item.id === state.selectedDataSourceId)) {
            state.selectedDataSourceId = LOCAL_DATASOURCE_ID;
        }
        renderDataSourceList(state.dataSources);
        updateCurrentDataSourceBanner();
        updateDataSourceStatus(`已加载 ${data.total || 0} 个数据源`, 'success');
    } catch (error) {
        state.dataSources = [LOCAL_DATASOURCE];
        state.selectedDataSourceId = LOCAL_DATASOURCE_ID;
        renderDataSourceList(state.dataSources);
        updateCurrentDataSourceBanner();
        updateDataSourceStatus('加载失败: ' + error.message, 'error');
        showToast('加载数据源失败: ' + error.message, 'error');
    } finally {
        if (withLoading) hideLoading();
    }
}

async function switchActiveDataSource(sourceId) {
    const nextSource = getAvailableDataSources().find((item) => item.id === sourceId);
    if (!nextSource) {
        showToast('数据源不存在', 'error');
        return;
    }
    state.selectedDataSourceId = sourceId;
    state.currentTable = null;
    state.currentPage = 0;
    state.totalRows = 0;
    updateCurrentDataSourceBanner();
    renderDataSourceList(getAvailableDataSources());
    document.getElementById('detail-title').textContent = '选择一个表查看详情';
    document.getElementById('table-description').innerHTML = '';
    document.querySelector('#columns-table tbody').innerHTML = '';
    document.querySelector('#data-table thead').innerHTML = '';
    document.querySelector('#data-table tbody').innerHTML = '';
    document.getElementById('relations-content').innerHTML = '';
    document.getElementById('data-info').textContent = '';
    document.getElementById('page-info').textContent = '';
    document.getElementById('prev-page').disabled = true;
    document.getElementById('next-page').disabled = true;
    await Promise.all([
        loadDatabaseSummary(),
        loadTableList(),
        loadERDiagram()
    ]);
    showToast(`已切换到数据源: ${nextSource.name}`, 'success');
}

async function testDataSource() {
    try {
        const payload = getDataSourceFormValues();
        validateDataSourceForm(payload);
        showLoading('正在测试数据源连接...');
        const result = await fetchAPI('/api/datasources/test', {
            method: 'POST',
            body: JSON.stringify(payload)
        });
        updateDataSourceStatus(result.message || '连接测试成功', 'success');
        showToast('数据源连接测试成功', 'success');
    } catch (error) {
        updateDataSourceStatus('测试失败: ' + error.message, 'error');
        showToast('数据源测试失败: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

async function saveDataSource() {
    try {
        const payload = getDataSourceFormValues();
        validateDataSourceForm(payload);
        showLoading('正在保存数据源...');
        const result = await fetchAPI('/api/datasources', {
            method: 'POST',
            body: JSON.stringify(payload)
        });
        updateDataSourceStatus(result.message || '保存成功', 'success');
        showToast('数据源已保存', 'success');
        await loadDataSources(false);
    } catch (error) {
        updateDataSourceStatus('保存失败: ' + error.message, 'error');
        showToast('保存数据源失败: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

async function testDataSourceById(sourceId) {
    const source = (state.dataSources || []).find((x) => x.id === sourceId);
    if (!source) {
        showToast('数据源不存在', 'error');
        return;
    }
    try {
        showLoading('正在测试数据源连接...');
        const result = await fetchAPI(`/api/datasources/${sourceId}/test`, { method: 'POST' });
        updateDataSourceStatus(`${source.name}: ${result.message || '连接测试成功'}`, 'success');
        showToast(`${source.name} 测试成功`, 'success');
    } catch (error) {
        updateDataSourceStatus(`${source.name}: 测试失败 - ${error.message}`, 'error');
        showToast(`${source.name} 测试失败: ${error.message}`, 'error');
    } finally {
        hideLoading();
    }
}

async function deleteDataSource(sourceId) {
    try {
        await fetchAPI(`/api/datasources/${sourceId}`, { method: 'DELETE' });
        if (state.selectedDataSourceId === sourceId) {
            state.selectedDataSourceId = LOCAL_DATASOURCE_ID;
            updateCurrentDataSourceBanner();
        }
        showToast('数据源已删除', 'success');
        await loadDataSources(false);
        if (state.selectedDataSourceId === LOCAL_DATASOURCE_ID) {
            await Promise.all([loadDatabaseSummary(), loadTableList(), loadERDiagram()]);
        }
    } catch (error) {
        showToast('删除失败: ' + error.message, 'error');
    }
}

// ============ 数据表管理模块 ============

/**
 * 加载数据库概览
 */
async function loadDatabaseSummary() {
    try {
        const data = await fetchAPI(buildDataSourceScopedUrl('/api/database/summary'));
        
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
        const data = await fetchAPI(buildDataSourceScopedUrl('/api/tables'));
        state.tables = data.tables;
        
        renderTableList(state.tables);
        if (isLocalDataSourceSelected()) {
            populateTableSelect(state.tables);
            populateTaggingTableSelect(state.tables);
        }
        
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
        const schema = await fetchAPI(buildDataSourceScopedUrl(`/api/tables/${tableName}/schema`));
        const readonly = !isLocalDataSourceSelected();
        
        // 渲染表描述
        const descEl = document.getElementById('table-description');
        descEl.innerHTML = `
            <div class="label">表描述</div>
            <div class="text">${schema.description || '<span style="color:#999">暂无描述</span>'}</div>
            ${readonly ? '<span style="color:#999">外部数据源暂不支持在线编辑描述</span>' : `<button class="edit-btn" onclick="editTableDescription('${tableName}', '${schema.description || ''}')">编辑</button>`}
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
                    ${readonly ? '<span style="color:#999">只读</span>' : `<button class="btn btn-small btn-secondary" 
                        onclick="editColumnDescription('${tableName}', '${col.name}', '${col.description || ''}')">
                        编辑
                    </button>`}
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
        const data = await fetchAPI(buildDataSourceScopedUrl(`/api/tables/${tableName}/data?limit=${state.pageSize}&offset=${offset}`));
        
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
        const relations = await fetchAPI(buildDataSourceScopedUrl(`/api/tables/${tableName}/related`));
        
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
    if (!isLocalDataSourceSelected()) {
        showToast('外部数据源当前仅支持只读浏览', 'error');
        return;
    }
    if (!hasPermission('tables.write')) {
        showToast('当前账号没有编辑元数据权限', 'error');
        return;
    }

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
    if (!isLocalDataSourceSelected()) {
        showToast('外部数据源当前仅支持只读浏览', 'error');
        return;
    }
    if (!hasPermission('tables.write')) {
        showToast('当前账号没有编辑元数据权限', 'error');
        return;
    }

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
            <div class="sql-step-board" id="metadata-step-board"></div>
        </div>
        <div class="final-result" id="metadata-final-result" style="display:none;"></div>
    `;
    document.getElementById('metadata-actions').style.display = 'none';
    
    const processBoardEl = document.getElementById('metadata-step-board');
    const finalResultEl = document.getElementById('metadata-final-result');
    resetMetadataProcessState();
    
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
                        handleMetadataStreamEvent(event, processBoardEl, finalResultEl);
                    } catch (e) {
                        console.error('解析事件失败:', e);
                    }
                }
            }
        }
        
        showToast('元数据生成完成', 'success');
        
    } catch (error) {
        showToast('生成失败: ' + error.message, 'error');
        appendSQLProcessLine(
            processBoardEl,
            'fatal-error',
            '执行错误',
            `❌ ${escapeHtml(error.message)}`,
            'error'
        );
    }
}

const metadataProcessState = {
    currentIterationKey: null,
    currentStepKey: null,
    usageTotal: null,
    usageRounds: []
};

function resetMetadataProcessState() {
    metadataProcessState.currentIterationKey = null;
    metadataProcessState.currentStepKey = null;
    metadataProcessState.usageTotal = null;
    metadataProcessState.usageRounds = [];
}

function upsertGenericUsageRound(processState, iteration, usage) {
    const item = {
        iteration: Number(iteration || (processState.usageRounds.length + 1)),
        input_tokens: Number(usage.input_tokens || 0),
        output_tokens: Number(usage.output_tokens || 0),
        total_tokens: Number(usage.total_tokens || ((usage.input_tokens || 0) + (usage.output_tokens || 0))),
        time: Number(usage.time || 0)
    };
    const idx = processState.usageRounds.findIndex((x) => x.iteration === item.iteration);
    if (idx >= 0) processState.usageRounds[idx] = item;
    else {
        processState.usageRounds.push(item);
        processState.usageRounds.sort((a, b) => a.iteration - b.iteration);
    }
}

function renderGenericUsageHtml(processState) {
    const rounds = processState.usageRounds || [];
    const total = processState.usageTotal;
    if (rounds.length === 0 && !total) return '<p><strong>Token统计：</strong>暂无</p>';

    const rows = rounds.map((r) =>
        `<li>第${escapeHtml(String(r.iteration))}轮：in ${escapeHtml(String(r.input_tokens))} / out ${escapeHtml(String(r.output_tokens))} / total ${escapeHtml(String(r.total_tokens))}${r.time ? ` / ${escapeHtml(r.time.toFixed(2))}s` : ''}</li>`
    ).join('');
    const totalLine = total
        ? `<p><strong>累计：</strong>in ${escapeHtml(String(total.input_tokens || 0))} / out ${escapeHtml(String(total.output_tokens || 0))} / total ${escapeHtml(String(total.total_tokens || 0))}${total.time ? ` / ${escapeHtml(Number(total.time).toFixed(2))}s` : ''}</p>`
        : '';

    return `
        <div class="result-section">
            <h5>Token 消耗</h5>
            <ul>${rows || '<li>暂无</li>'}</ul>
            ${totalLine}
        </div>
    `;
}

/**
 * 处理元数据流式事件
 */
function handleMetadataStreamEvent(event, processBoardEl, finalResultEl) {
    switch (event.type) {
        case 'start':
            appendSQLProcessLine(
                processBoardEl,
                'overview',
                '初始化',
                `<span class="line-tag">🚀 启动</span> ${escapeHtml(event.message || '')}`,
                'start'
            );
            break;
        
        case 'iteration_start':
            metadataProcessState.currentIterationKey = `meta-iter-${event.iteration || 'x'}`;
            metadataProcessState.currentStepKey = metadataProcessState.currentIterationKey;
            appendSQLProcessLine(
                processBoardEl,
                metadataProcessState.currentIterationKey,
                `第 ${event.iteration || '?'} 轮推理`,
                `<span class="line-tag">🔁 轮次</span> ${escapeHtml(event.message || '')}`,
                'iteration'
            );
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
            
            appendSQLProcessLine(
                processBoardEl,
                metadataProcessState.currentIterationKey || 'overview',
                '元数据缺失分析',
                `
                <div>${missingTableBadge}${missingColBadge}</div>
                <div>${missingColList || ''}</div>
                `,
                'step'
            );
            break;
        
        case 'skill_start':
            appendSQLProcessLine(
                processBoardEl,
                metadataProcessState.currentIterationKey || 'overview',
                '技能调用',
                `<span class="line-tag">🧩 技能</span> ${escapeHtml(event.skill_name || '')}`,
                'skill'
            );
            break;
            
        case 'step':
            metadataProcessState.currentStepKey = `meta-step-${event.step || 'x'}`;
            appendSQLProcessLine(
                processBoardEl,
                metadataProcessState.currentStepKey,
                escapeHtml(event.title || '步骤'),
                `<span class="line-tag">📌 ${escapeHtml(event.status || 'running')}</span> ${escapeHtml(event.message || '')}`,
                'step'
            );
            break;
            
        case 'tool_result':
            // 显示工具调用详情，包含入参和出参
            const skillBadge = event.is_skill_tool ? 
                `<span class="skill-badge">Skill: ${event.skill_name || 'Database Schema Analysis'}</span>` : '';
            const inputParams = event.input ? 
                `<div class="tool-input"><strong>入参:</strong> <code>${escapeHtml(JSON.stringify(event.input))}</code></div>` : '';
            
            appendSQLProcessLine(
                processBoardEl,
                metadataProcessState.currentStepKey || metadataProcessState.currentIterationKey || 'overview',
                `工具执行: ${escapeHtml(event.tool || '')}`,
                `
                <div>${skillBadge}</div>
                ${inputParams}
                <pre class="sql-step-pre">${escapeHtml(event.result || '')}</pre>
                `,
                'tool'
            );
            break;
            
        case 'thinking':
            appendSQLProcessLine(
                processBoardEl,
                metadataProcessState.currentStepKey || metadataProcessState.currentIterationKey || 'overview',
                '模型思考',
                `<span class="line-tag">💭 思考</span> ${escapeHtml(event.message || '')}`,
                'thinking'
            );
            break;
            
        case 'llm_response':
            appendSQLProcessLine(
                processBoardEl,
                metadataProcessState.currentStepKey || metadataProcessState.currentIterationKey || 'overview',
                '模型响应',
                `<pre class="sql-step-pre">${escapeHtml(event.content || '')}</pre>`,
                'llm'
            );
            break;

        case 'usage':
            metadataProcessState.usageTotal = event.usage_total || event.usage || null;
            upsertGenericUsageRound(metadataProcessState, event.iteration, event.usage || {});
            break;
            
        case 'result':
            state.generatedMetadata = event.data;
            if (event.data && event.data.usage) {
                metadataProcessState.usageTotal = event.data.usage;
            }
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
                    ${renderGenericUsageHtml(metadataProcessState)}
                `;
                document.getElementById('metadata-actions').style.display = 'flex';
            }
            break;
            
        case 'error':
            appendSQLProcessLine(
                processBoardEl,
                'fatal-error',
                '执行错误',
                `❌ ${escapeHtml(event.message || '')}`,
                'error'
            );
            break;
            
        case 'end':
            appendSQLProcessLine(
                processBoardEl,
                'overview',
                '流程结束',
                `<span class="line-tag">🏁</span> ${escapeHtml(event.message || '')}`,
                'end'
            );
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

function renderMarkdownToHtml(markdownText) {
    const source = String(markdownText || '').replace(/\r\n/g, '\n');
    const codeBlocks = [];

    const withPlaceholders = source.replace(/```[\w-]*\n([\s\S]*?)```/g, (_, code) => {
        const index = codeBlocks.push(
            `<pre class="sql-explain-code"><code>${escapeHtml(code)}</code></pre>`
        ) - 1;
        return `@@CODE_BLOCK_${index}@@`;
    });

    const lines = withPlaceholders.split('\n');
    let html = '';
    let i = 0;
    let inUl = false;
    let inOl = false;

    const closeLists = () => {
        if (inUl) {
            html += '</ul>';
            inUl = false;
        }
        if (inOl) {
            html += '</ol>';
            inOl = false;
        }
    };

    const renderTable = (tableLines) => {
        if (tableLines.length < 2) return `<p>${escapeHtml(tableLines.join('\n'))}</p>`;
        const separator = tableLines[1];
        if (!/^\|\s*[-:| ]+\s*\|$/.test(separator)) return `<p>${escapeHtml(tableLines.join('\n'))}</p>`;

        const parseRow = (line) => line.split('|').slice(1, -1).map((cell) => cell.trim());
        const headerCells = parseRow(tableLines[0]);
        const bodyRows = tableLines.slice(2).map(parseRow);

        let tableHtml = '<table class="sql-explain-table"><thead><tr>';
        tableHtml += headerCells.map((cell) => `<th>${escapeHtml(cell)}</th>`).join('');
        tableHtml += '</tr></thead><tbody>';
        tableHtml += bodyRows.map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join('')}</tr>`).join('');
        tableHtml += '</tbody></table>';
        return tableHtml;
    };

    while (i < lines.length) {
        const line = lines[i];
        const trimmed = line.trim();

        if (!trimmed) {
            closeLists();
            i += 1;
            continue;
        }

        if (/^@@CODE_BLOCK_\d+@@$/.test(trimmed)) {
            closeLists();
            html += `<div class="sql-explain-block">${trimmed}</div>`;
            i += 1;
            continue;
        }

        const headingMatch = trimmed.match(/^(#{1,6})\s+(.*)$/);
        if (headingMatch) {
            closeLists();
            const level = headingMatch[1].length;
            html += `<h${level}>${escapeHtml(headingMatch[2])}</h${level}>`;
            i += 1;
            continue;
        }

        if (/^(-{3,}|\*{3,})$/.test(trimmed)) {
            closeLists();
            html += '<hr />';
            i += 1;
            continue;
        }

        const ulMatch = trimmed.match(/^[-*]\s+(.*)$/);
        if (ulMatch) {
            if (!inUl) {
                closeLists();
                html += '<ul>';
                inUl = true;
            }
            html += `<li>${escapeHtml(ulMatch[1])}</li>`;
            i += 1;
            continue;
        }

        const olMatch = trimmed.match(/^\d+\.\s+(.*)$/);
        if (olMatch) {
            if (!inOl) {
                closeLists();
                html += '<ol>';
                inOl = true;
            }
            html += `<li>${escapeHtml(olMatch[1])}</li>`;
            i += 1;
            continue;
        }

        if (/^\|.*\|$/.test(trimmed)) {
            closeLists();
            const tableLines = [];
            while (i < lines.length && /^\|.*\|$/.test(lines[i].trim())) {
                tableLines.push(lines[i].trim());
                i += 1;
            }
            html += renderTable(tableLines);
            continue;
        }

        closeLists();
        html += `<p>${escapeHtml(trimmed)}</p>`;
        i += 1;
    }

    closeLists();

    return html.replace(/@@CODE_BLOCK_(\d+)@@/g, (_, index) => codeBlocks[Number(index)] || '');
}

/**
 * 应用元数据到数据库
 */
async function applyMetadata() {
    if (!state.generatedMetadata) return;
    if (!hasPermission('tables.write')) {
        showToast('当前账号没有应用元数据权限', 'error');
        return;
    }
    
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
            <div class="sql-step-board" id="sql-step-board"></div>
        </div>
        <div class="final-result" id="sql-final-result" style="display:none;"></div>
    `;
    document.getElementById('sql-actions').style.display = 'none';
    document.getElementById('sql-execution-result').style.display = 'none';
    
    const processBoardEl = document.getElementById('sql-step-board');
    const finalResultEl = document.getElementById('sql-final-result');
    resetSQLProcessState();
    
    try {
        // 使用 SSE 流式请求
        const inference = getSQLInferenceOptions();
        const response = await fetch(buildDataSourceScopedUrl('/api/agent/sql/generate/stream'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                requirement,
                context: context || null,
                temperature: inference.temperature
            })
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
                        handleSQLStreamEvent(event, processBoardEl, finalResultEl);
                    } catch (e) {
                        console.error('解析事件失败:', e);
                    }
                }
            }
        }
        
        showToast('SQL生成完成', 'success');
        
    } catch (error) {
        showToast('生成失败: ' + error.message, 'error');
        appendSQLProcessLine(
            processBoardEl,
            'fatal-error',
            '执行错误',
            `❌ ${escapeHtml(error.message)}`,
            'error'
        );
    }
}

const sqlProcessState = {
    currentIterationKey: null,
    currentStepKey: null,
    usageTotal: null,
    usageRounds: [],
    renderedParsedBlocks: new Set()
};

function resetSQLProcessState() {
    sqlProcessState.currentIterationKey = null;
    sqlProcessState.currentStepKey = null;
    sqlProcessState.usageTotal = null;
    sqlProcessState.usageRounds = [];
    sqlProcessState.renderedParsedBlocks = new Set();
    renderSQLTokenStatsPanel();
}

function getNowTimeText() {
    return new Date().toLocaleTimeString('zh-CN', { hour12: false });
}

function normalizeText(value) {
    if (value === null || value === undefined) return '';
    if (typeof value === 'string') return value;
    try {
        return JSON.stringify(value, null, 2);
    } catch (e) {
        return String(value);
    }
}

function truncateText(value, maxLength = 900) {
    const text = normalizeText(value);
    if (text.length <= maxLength) return text;
    return text.slice(0, maxLength) + '\n...（已截断）';
}

function renderCollapsiblePre(label, value, options = {}) {
    const text = normalizeText(value);
    const previewLength = Number(options.previewLength || 220);
    const open = Boolean(options.open);
    if (!text) return '';

    const preview = text.length > previewLength ? `${text.slice(0, previewLength)}...` : text;
    return `
        <details class="sql-step-details"${open ? ' open' : ''}>
            <summary>${escapeHtml(label)}${text.length > previewLength ? `：${escapeHtml(preview)}` : ''}</summary>
            <pre class="sql-step-pre">${escapeHtml(text)}</pre>
        </details>
    `;
}

function getParsedBlockTypeLabel(type) {
    const normalized = String(type || '').toLowerCase();
    if (normalized === 'thinking') return '思考';
    if (normalized === 'text') return '回复';
    if (normalized === 'tool_use') return '工具调用';
    if (normalized === 'reasoning') return '推理';
    return normalized || '未知类型';
}

function getParsedBlockMainText(block) {
    if (!block || typeof block !== 'object') return '';
    return normalizeText(
        block.thinking
        || block.text
        || block.content
        || block.reasoning
        || block.reasoning_content
        || ''
    ).trim();
}

function renderParsedModelBlock(block, index) {
    const blockType = String(block?.type || 'unknown').toLowerCase();
    const title = getParsedBlockTypeLabel(blockType);

    if (blockType === 'tool_use') {
        const toolName = String(block?.name || 'unknown');
        const toolInput = block?.input ?? {};
        return `
            <div class="sql-block-item sql-block-item-tool">
                <div class="sql-block-title">
                    <span class="line-tag">Block ${index + 1}</span>
                    <span class="sql-block-kind">${escapeHtml(title)}</span>
                </div>
                <div class="sql-block-content">
                    <div><span class="line-tag">工具名</span> ${escapeHtml(toolName)}</div>
                    ${renderCollapsiblePre('查看工具参数', toolInput, { previewLength: 220, open: false })}
                </div>
            </div>
        `;
    }

    const mainText = getParsedBlockMainText(block);
    return `
        <div class="sql-block-item sql-block-item-${escapeHtml(blockType || 'unknown')}">
            <div class="sql-block-title">
                <span class="line-tag">Block ${index + 1}</span>
                <span class="sql-block-kind">${escapeHtml(title)}</span>
            </div>
            <div class="sql-block-content">
                <div class="sql-block-text">${escapeHtml(mainText || '本块没有可展示内容')}</div>
            </div>
        </div>
    `;
}

function ensureSQLProcessCard(container, cardKey, title, type = 'default') {
    if (!container) return null;
    const escapedKey = cardKey.replace(/[^a-zA-Z0-9_-]/g, '-');
    const domId = `sql-process-card-${escapedKey}`;
    let card = document.getElementById(domId);

    if (!card) {
        card = document.createElement('section');
        card.id = domId;
        card.className = `sql-step-card sql-step-${type}`;
        card.innerHTML = `
            <div class="sql-step-header">
                <div class="sql-step-title">${escapeHtml(title)}</div>
                <div class="sql-step-meta">
                    <span class="sql-step-status">进行中</span>
                    <span class="sql-step-time">${getNowTimeText()}</span>
                </div>
            </div>
            <div class="sql-step-stream"></div>
        `;
        container.appendChild(card);
    } else if (type !== 'default') {
        card.classList.add(`sql-step-${type}`);
    }

    return card;
}

function updateSQLProcessCardStatus(card, statusText, statusType) {
    if (!card) return;
    const statusEl = card.querySelector('.sql-step-status');
    const timeEl = card.querySelector('.sql-step-time');
    if (statusEl) statusEl.textContent = statusText;
    if (timeEl) timeEl.textContent = getNowTimeText();

    card.classList.remove('is-done', 'is-error', 'is-running');
    if (statusType === 'done') card.classList.add('is-done');
    else if (statusType === 'error') card.classList.add('is-error');
    else card.classList.add('is-running');
}

function appendSQLProcessLine(container, cardKey, title, htmlLine, cardType = 'default') {
    const card = ensureSQLProcessCard(container, cardKey, title, cardType);
    if (!card) return;
    const streamEl = card.querySelector('.sql-step-stream');
    if (!streamEl) return;

    const line = document.createElement('div');
    line.className = 'sql-step-line';
    line.innerHTML = htmlLine;
    streamEl.appendChild(line);

    updateSQLProcessCardStatus(card, '进行中', 'running');
    container.scrollTop = container.scrollHeight;
}

function formatUsageSummary(usage) {
    if (!usage) return 'Token统计：暂无';
    const input = Number(usage.input_tokens || 0);
    const output = Number(usage.output_tokens || 0);
    const total = Number(usage.total_tokens || (input + output));
    const time = Number(usage.time || 0);
    return `Token统计：输入 ${input} / 输出 ${output} / 总计 ${total}${time ? ` / 耗时 ${time.toFixed(2)}s` : ''}`;
}

function upsertSQLUsageRound(iteration, usage) {
    const item = {
        iteration: Number(iteration || (sqlProcessState.usageRounds.length + 1)),
        input_tokens: Number(usage.input_tokens || 0),
        output_tokens: Number(usage.output_tokens || 0),
        total_tokens: Number(usage.total_tokens || ((usage.input_tokens || 0) + (usage.output_tokens || 0))),
        time: Number(usage.time || 0)
    };

    const idx = sqlProcessState.usageRounds.findIndex((x) => x.iteration === item.iteration);
    if (idx >= 0) {
        sqlProcessState.usageRounds[idx] = item;
    } else {
        sqlProcessState.usageRounds.push(item);
        sqlProcessState.usageRounds.sort((a, b) => a.iteration - b.iteration);
    }
}

function renderSQLTokenStatsPanel() {
    const panelEl = document.getElementById('sql-token-stats');
    if (!panelEl) return;

    const rounds = sqlProcessState.usageRounds || [];
    const total = sqlProcessState.usageTotal;

    if (rounds.length === 0 && !total) {
        panelEl.innerHTML = '<div class="sql-token-empty">暂无数据</div>';
        return;
    }

    let html = '';
    if (rounds.length > 0) {
        html += rounds.map((r) => `
            <div class="sql-token-row">
                <span class="label">第${escapeHtml(String(r.iteration))}轮</span>
                <span class="value">in ${escapeHtml(String(r.input_tokens))} / out ${escapeHtml(String(r.output_tokens))} / total ${escapeHtml(String(r.total_tokens))}${r.time ? ` / ${escapeHtml(r.time.toFixed(2))}s` : ''}</span>
            </div>
        `).join('');
    }

    if (total) {
        html += `
            <div class="sql-token-total">
                累计：in ${escapeHtml(String(total.input_tokens || 0))} / out ${escapeHtml(String(total.output_tokens || 0))} / total ${escapeHtml(String(total.total_tokens || 0))}${total.time ? ` / ${escapeHtml(Number(total.time).toFixed(2))}s` : ''}
            </div>
        `;
    }

    panelEl.innerHTML = html;
}

function getCurrentSQLIterationCardKey() {
    return sqlProcessState.currentIterationKey || 'overview';
}

/**
 * 处理SQL流式事件
 */
function handleSQLStreamEvent(event, processBoardEl, finalResultEl) {
    switch (event.type) {
        case 'start':
            appendSQLProcessLine(
                processBoardEl,
                'overview',
                '步骤 1: 请求初始化',
                `<span class="line-tag">🚀 启动</span> ${escapeHtml(event.message || '')}`,
                'start'
            );
            break;
        
        case 'iteration_start':
            sqlProcessState.currentIterationKey = `iteration-${event.iteration || 'x'}`;
            sqlProcessState.currentStepKey = sqlProcessState.currentIterationKey;
            appendSQLProcessLine(
                processBoardEl,
                sqlProcessState.currentIterationKey,
                `步骤 2: 第 ${event.iteration || '?'} 轮推理`,
                `<span class="line-tag">🔁 轮次</span> ${escapeHtml(event.message || '开始新一轮推理')}`,
                'iteration'
            );
            break;

        case 'model_request':
            appendSQLProcessLine(
                processBoardEl,
                getCurrentSQLIterationCardKey(),
                `步骤 2: 第 ${escapeHtml(String(event.iteration || '?'))} 轮推理`,
                `
                <div><span class="line-tag">📤 模型请求</span> 本轮实际下发给模型的请求体</div>
                ${renderCollapsiblePre('查看本轮完整请求体', event.payload, { previewLength: 240, open: false })}
                `,
                'iteration'
            );
            break;

        case 'parsed_model_blocks': {
            const cardKey = getCurrentSQLIterationCardKey();
            sqlProcessState.renderedParsedBlocks.add(cardKey);
            const blocks = Array.isArray(event.blocks) ? event.blocks : [];
            appendSQLProcessLine(
                processBoardEl,
                cardKey,
                `步骤 2: 第 ${escapeHtml(String(event.iteration || '?'))} 轮推理`,
                `
                <div><span class="line-tag">🧩 解析块</span> 已按 thinking / text / tool_use 规范化展示</div>
                ${blocks.map((block, index) => renderParsedModelBlock(block, index)).join('')}
                `,
                'iteration'
            );
            break;
        }
        
        case 'skill_start':
            appendSQLProcessLine(
                processBoardEl,
                sqlProcessState.currentIterationKey || 'overview',
                '步骤 2: 轮次推理',
                `<span class="line-tag">🧩 技能</span> ${escapeHtml(event.skill_name || '未命名技能')}`,
                'iteration'
            );
            break;
            
        case 'step':
            sqlProcessState.currentStepKey = getCurrentSQLIterationCardKey();
            appendSQLProcessLine(
                processBoardEl,
                sqlProcessState.currentStepKey,
                `步骤 2: 第 ${escapeHtml(String(event.iteration || '?'))} 轮推理`,
                `
                <div><span class="line-tag">📌 步骤</span> ${escapeHtml(event.title || '步骤更新')}</div>
                <div><span class="line-tag">📌 状态</span> ${escapeHtml(event.status || 'running')}</div>
                <div>${escapeHtml(event.message || '')}</div>
                `,
                'step'
            );
            updateSQLProcessCardStatus(
                ensureSQLProcessCard(processBoardEl, sqlProcessState.currentStepKey, `步骤 2: 第 ${event.iteration || '?'} 轮推理`, 'iteration'),
                event.status === 'done' ? '已完成' : '进行中',
                event.status === 'done' ? 'done' : 'running'
            );
            break;
            
        case 'tool_result':
            appendSQLProcessLine(
                processBoardEl,
                getCurrentSQLIterationCardKey(),
                `步骤 2: 第 ${escapeHtml(String(event.iteration || '?'))} 轮推理`,
                `
                <div><span class="line-tag">🛠️ 工具</span> ${escapeHtml(event.tool || 'unknown')}</div>
                ${renderCollapsiblePre('查看完整入参', event.input, { previewLength: 180 })}
                ${renderCollapsiblePre('查看完整出参', event.result, { previewLength: 260 })}
                `,
                'iteration'
            );
            break;
            
        case 'thinking':
            if (sqlProcessState.renderedParsedBlocks.has(getCurrentSQLIterationCardKey())) {
                break;
            }
            appendSQLProcessLine(
                processBoardEl,
                getCurrentSQLIterationCardKey(),
                `步骤 2: 第 ${escapeHtml(String(event.iteration || '?'))} 轮推理`,
                `
                <div><span class="line-tag">💭 思考</span></div>
                ${renderCollapsiblePre('查看完整思考', event.message || '', { previewLength: 280, open: true })}
                `,
                'iteration'
            );
            break;
            
        case 'llm_response':
            if (sqlProcessState.renderedParsedBlocks.has(getCurrentSQLIterationCardKey())) {
                break;
            }
            appendSQLProcessLine(
                processBoardEl,
                getCurrentSQLIterationCardKey(),
                `步骤 2: 第 ${escapeHtml(String(event.iteration || '?'))} 轮推理`,
                `
                <div><span class="line-tag">🤖 回包</span></div>
                ${renderCollapsiblePre('查看完整模型回包', event.content, { previewLength: 280, open: true })}
                `,
                'iteration'
            );
            break;

        case 'raw_model_response':
            break;
            
        case 'result':
            state.generatedSQL = event.data;
            if (event.data && event.data.usage) {
                sqlProcessState.usageTotal = event.data.usage;
            }
            renderSQLTokenStatsPanel();
            finalResultEl.style.display = 'block';
            appendSQLProcessLine(
                processBoardEl,
                'final-result',
                '步骤 3: 结果汇总',
                event.data && event.data.sql
                    ? '<span class="line-tag">✅ 完成</span> 已产出最终 SQL'
                    : '<span class="line-tag">⚠️ 完成</span> 未解析到可用 SQL，请检查上方步骤',
                'result'
            );
            updateSQLProcessCardStatus(
                ensureSQLProcessCard(processBoardEl, 'final-result', '步骤 3: 结果汇总', 'result'),
                '已完成',
                'done'
            );
            
            if (event.data.sql) {
                const usage = event.data.usage || sqlProcessState.usageTotal;
                finalResultEl.innerHTML = `
                    <h4>✅ 生成的SQL</h4>
                    <div class="sql-code">
                        <pre>${escapeHtml(event.data.sql)}</pre>
                    </div>
                    <div class="explanation">
                        <h5>说明</h5>
                        <p>${event.data.explanation || '无'}</p>
                        <p><strong>${escapeHtml(formatUsageSummary(usage))}</strong></p>
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

        case 'usage': {
            const usage = event.usage || {};
            const usageTotal = event.usage_total || usage;
            sqlProcessState.usageTotal = usageTotal;
            upsertSQLUsageRound(event.iteration, usage);
            renderSQLTokenStatsPanel();
            break;
        }
            
        case 'error':
            appendSQLProcessLine(
                processBoardEl,
                getCurrentSQLIterationCardKey(),
                `步骤 2: 第 ${escapeHtml(String(event.iteration || '?'))} 轮推理`,
                `
                <div><span class="line-tag">❌ 错误</span> ${escapeHtml(event.message || '')}</div>
                ${event.tool ? `<div><span class="line-tag">🛠️ 工具</span> ${escapeHtml(event.tool)}</div>` : ''}
                ${event.tool_input ? renderCollapsiblePre('查看完整工具入参', event.tool_input, { previewLength: 200 }) : ''}
                ${event.context ? renderCollapsiblePre('查看完整上下文', event.context, { previewLength: 220 }) : ''}
                ${event.detail ? renderCollapsiblePre('查看完整异常详情', event.detail, { previewLength: 260, open: true }) : ''}
                `,
                'iteration'
            );
            updateSQLProcessCardStatus(
                ensureSQLProcessCard(processBoardEl, getCurrentSQLIterationCardKey(), `步骤 2: 第 ${event.iteration || '?'} 轮推理`, 'iteration'),
                '失败',
                'error'
            );
            break;
            
        case 'end':
            appendSQLProcessLine(
                processBoardEl,
                'overview',
                '步骤 1: 请求初始化',
                `<span class="line-tag">🏁 结束</span> ${escapeHtml(event.message || '')}`,
                'end'
            );
            updateSQLProcessCardStatus(
                ensureSQLProcessCard(processBoardEl, 'overview', '步骤 1: 请求初始化', 'start'),
                '已完成',
                'done'
            );
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
        const result = await fetchAPI(buildDataSourceScopedUrl('/api/sql/execute'), {
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

    showModal('SQL解释（流式）', `
        <div id="sql-explain-stream-content" class="sql-explain-stream-content">
            正在建立连接...
        </div>
    `);
    document.getElementById('modal').classList.add('modal-explain');

    const streamContentEl = document.getElementById('sql-explain-stream-content');
    if (!streamContentEl) {
        showToast('解释窗口渲染失败，请重试', 'error');
        return;
    }

    const renderFinalExplanation = (explanation) => {
        streamContentEl.innerHTML = `
            <div class="sql-explain-title">解释完成：</div>
            <div class="sql-explain-rich">${renderMarkdownToHtml(explanation || '未返回解释内容')}</div>
        `;
    };

    const renderLine = (text) => {
        streamContentEl.innerHTML += `<div>${escapeHtml(text)}</div>`;
        streamContentEl.scrollTop = streamContentEl.scrollHeight;
    };

    streamContentEl.innerHTML = '';
    renderLine('开始请求解释...');

    try {
        const response = await fetch('/api/agent/sql/explain/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql: state.generatedSQL.sql })
        });

        if (!response.ok) {
            const errText = await response.text().catch(() => '');
            throw new Error(errText || `HTTP ${response.status}`);
        }

        // 浏览器或代理不支持流式时，降级到非流式接口
        if (!response.body || typeof response.body.getReader !== 'function') {
            const fallback = await fetchAPI('/api/agent/sql/explain', {
                method: 'POST',
                body: JSON.stringify({ sql: state.generatedSQL.sql })
            });
            renderFinalExplanation(fallback.explanation || '未返回解释内容');
            return;
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let receivedResult = false;

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop();

            for (const line of lines) {
                if (!line.startsWith('data: ')) continue;
                const event = JSON.parse(line.slice(6));

                if (event.type === 'start') {
                    renderLine(event.message || '开始解释...');
                } else if (event.type === 'thinking') {
                    renderLine(event.message || '分析中...');
                } else if (event.type === 'result') {
                    const explanation = event.data && event.data.explanation ? event.data.explanation : '未返回解释内容';
                    renderFinalExplanation(explanation);
                    receivedResult = true;
                } else if (event.type === 'error') {
                    renderLine(`❌ ${event.message || '解释失败'}`);
                } else if (event.type === 'end') {
                    renderLine(event.message || '结束');
                }
            }
        }

        // 兜底：若流式未返回result事件，则走一次非流式解释
        if (!receivedResult) {
            const fallback = await fetchAPI('/api/agent/sql/explain', {
                method: 'POST',
                body: JSON.stringify({ sql: state.generatedSQL.sql })
            });
            renderFinalExplanation(fallback.explanation || '未返回解释内容');
        }
    } catch (error) {
        renderLine(`❌ 解释失败: ${error.message}`);
        showToast('解释失败: ' + error.message, 'error');
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
        const data = await fetchAPI(buildDataSourceScopedUrl('/api/database/er-diagram'));
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
            <div class="sql-step-board" id="tagging-step-board"></div>
        </div>
        <div class="final-result" id="tagging-final-result" style="display:none;"></div>
    `;
    document.getElementById('tagging-actions').style.display = 'none';
    
    const processBoardEl = document.getElementById('tagging-step-board');
    const finalResultEl = document.getElementById('tagging-final-result');
    resetTaggingProcessState();
    
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
                        handleTaggingStreamEvent(event, processBoardEl, finalResultEl, tableName);
                    } catch (e) {
                        console.error('解析事件失败:', e);
                    }
                }
            }
        }
        
        showToast('标签生成完成', 'success');
        
    } catch (error) {
        showToast('生成失败: ' + error.message, 'error');
        appendSQLProcessLine(
            processBoardEl,
            'fatal-error',
            '执行错误',
            `❌ ${escapeHtml(error.message)}`,
            'error'
        );
    }
}

const taggingProcessState = {
    currentIterationKey: null,
    currentStepKey: null,
    usageTotal: null,
    usageRounds: []
};

function resetTaggingProcessState() {
    taggingProcessState.currentIterationKey = null;
    taggingProcessState.currentStepKey = null;
    taggingProcessState.usageTotal = null;
    taggingProcessState.usageRounds = [];
}

/**
 * 处理标签生成流式事件
 */
function handleTaggingStreamEvent(event, processBoardEl, finalResultEl, tableName) {
    switch (event.type) {
        case 'start':
            appendSQLProcessLine(
                processBoardEl,
                'overview',
                '初始化',
                `<span class="line-tag">🚀 启动</span> ${escapeHtml(event.message || '')}`,
                'start'
            );
            break;
        
        case 'step':
            taggingProcessState.currentStepKey = `tag-step-${event.step || 'x'}`;
            appendSQLProcessLine(
                processBoardEl,
                taggingProcessState.currentStepKey,
                escapeHtml(event.title || '步骤'),
                `<span class="line-tag">📌 ${escapeHtml(event.status || 'running')}</span> ${escapeHtml(event.message || '')}`,
                'step'
            );
            break;
            
        case 'tool_result':
            appendSQLProcessLine(
                processBoardEl,
                taggingProcessState.currentStepKey || taggingProcessState.currentIterationKey || 'overview',
                `工具执行: ${escapeHtml(event.tool || '')}`,
                `<pre class="sql-step-pre">${escapeHtml((event.result || '').substring(0, 800))}${(event.result || '').length > 800 ? '\n...（已截断）' : ''}</pre>`,
                'tool'
            );
            break;
            
        case 'thinking':
            appendSQLProcessLine(
                processBoardEl,
                taggingProcessState.currentStepKey || taggingProcessState.currentIterationKey || 'overview',
                '模型思考',
                `<span class="line-tag">💭 思考</span> ${escapeHtml(event.message || '')}`,
                'thinking'
            );
            break;

        case 'iteration_start':
            taggingProcessState.currentIterationKey = `tag-iter-${event.iteration || 'x'}`;
            taggingProcessState.currentStepKey = taggingProcessState.currentIterationKey;
            appendSQLProcessLine(
                processBoardEl,
                taggingProcessState.currentIterationKey,
                `第 ${event.iteration || '?'} 轮推理`,
                `<span class="line-tag">🔁 轮次</span> ${escapeHtml(event.message || '')}`,
                'iteration'
            );
            break;

        case 'usage':
            taggingProcessState.usageTotal = event.usage_total || event.usage || null;
            upsertGenericUsageRound(taggingProcessState, event.iteration, event.usage || {});
            break;
            
        case 'result':
            state.generatedTags = { ...event.data, table_name: tableName };
            if (event.data && event.data.usage) {
                taggingProcessState.usageTotal = event.data.usage;
            }
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
            
            tagsHtml += '</div>' + renderGenericUsageHtml(taggingProcessState);
            finalResultEl.innerHTML = tagsHtml;
            document.getElementById('tagging-actions').style.display = 'flex';
            break;
            
        case 'error':
            appendSQLProcessLine(
                processBoardEl,
                'fatal-error',
                '执行错误',
                `❌ ${escapeHtml(event.message || '')}`,
                'error'
            );
            break;
            
        case 'end':
            appendSQLProcessLine(
                processBoardEl,
                'overview',
                '流程结束',
                `<span class="line-tag">🏁</span> ${escapeHtml(event.message || '')}`,
                'end'
            );
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
        const result = await fetchAPI(buildDataSourceScopedUrl('/api/agent/sql-validation/generate'), {
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
        const inference = getValidationInferenceOptions();
        const response = await fetch(buildDataSourceScopedUrl('/api/agent/sql-validation/validate/stream'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                sql: sql,
                temperature: inference.temperature
            })
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

        case 'usage':
            thinkingEl.innerHTML += `
                <div class="thinking-item">🧮 Token统计：输入 ${event.usage_total?.input_tokens || 0} / 输出 ${event.usage_total?.output_tokens || 0} / 总计 ${event.usage_total?.total_tokens || 0}</div>
            `;
            if (processContent) {
                addProcessItem(
                    processContent,
                    'api',
                    'Token统计',
                    `输入 ${event.usage_total?.input_tokens || 0} / 输出 ${event.usage_total?.output_tokens || 0} / 总计 ${event.usage_total?.total_tokens || 0}`
                );
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

document.addEventListener('DOMContentLoaded', async () => {
    // 认证相关按钮
    const loginBtn = document.getElementById('auth-login-btn');
    if (loginBtn) {
        loginBtn.addEventListener('click', login);
    }
    const passwordEl = document.getElementById('auth-password');
    if (passwordEl) {
        passwordEl.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') login();
        });
    }
    const logoutBtn = document.getElementById('logout-btn');
    if (logoutBtn) {
        logoutBtn.addEventListener('click', logout);
    }

    const authed = await checkAuthSession();
    if (authed) {
        initializeAppData();
    }
    
    // 导航标签切换（事件委托，避免动态显示/隐藏导致绑定失效）
    const navEl = document.querySelector('.nav');
    if (navEl) {
        navEl.addEventListener('click', (event) => {
            const target = event.target instanceof Element ? event.target : null;
            if (!target) return;
            const btn = target.closest('.nav-btn[data-tab]');
            if (!btn || btn.disabled) return;
            switchMainTab(btn.dataset.tab);
        });
    }

    // 确保初始化后存在可见激活页
    const visibleTabs = getVisibleTabButtons();
    const initialActiveTab = visibleTabs.find((btn) => btn.classList.contains('active'))?.dataset.tab;
    const firstVisibleTab = visibleTabs[0]?.dataset.tab;
    switchMainTab(initialActiveTab || firstVisibleTab);
    
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

    // 模型配置按钮
    const loadModelConfigBtn = document.getElementById('load-model-config-btn');
    if (loadModelConfigBtn) {
        loadModelConfigBtn.addEventListener('click', () => loadModelConfig());
    }

    const testModelConfigBtn = document.getElementById('test-model-config-btn');
    if (testModelConfigBtn) {
        testModelConfigBtn.addEventListener('click', testModelConfig);
    }

    const saveModelConfigBtn = document.getElementById('save-model-config-btn');
    if (saveModelConfigBtn) {
        saveModelConfigBtn.addEventListener('click', saveModelConfig);
    }

    const toggleApiKeyBtn = document.getElementById('toggle-api-key-visibility');
    if (toggleApiKeyBtn) {
        toggleApiKeyBtn.addEventListener('click', toggleApiKeyVisibility);
    }

    const refreshDataSourcesBtn = document.getElementById('refresh-datasources-btn');
    if (refreshDataSourcesBtn) {
        refreshDataSourcesBtn.addEventListener('click', () => loadDataSources());
    }

    const testDataSourceBtn = document.getElementById('test-datasource-btn');
    if (testDataSourceBtn) {
        testDataSourceBtn.addEventListener('click', testDataSource);
    }

    const saveDataSourceBtn = document.getElementById('save-datasource-btn');
    if (saveDataSourceBtn) {
        saveDataSourceBtn.addEventListener('click', saveDataSource);
    }

    const dataSourceListEl = document.getElementById('datasource-list');
    if (dataSourceListEl) {
        dataSourceListEl.addEventListener('click', (event) => {
            const target = event.target instanceof Element ? event.target : null;
            if (!target) return;
            const btn = target.closest('button[data-action][data-id]');
            if (!btn) return;
            const action = btn.getAttribute('data-action');
            const id = btn.getAttribute('data-id');
            if (!id) return;
            if (action === 'use') {
                switchActiveDataSource(id);
            } else if (action === 'test') {
                testDataSourceById(id);
            } else if (action === 'delete') {
                deleteDataSource(id);
            }
        });
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
