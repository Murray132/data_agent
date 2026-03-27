# -*- coding: utf-8 -*-
"""
金融数据集初始化脚本
创建一个包含25+张表的金融领域SQLite数据库
包含完整的元数据描述和样例数据
"""

import sqlite3
import random
from datetime import datetime, timedelta
from pathlib import Path

# 数据库路径
DB_PATH = Path(__file__).parent / "finance.db"

# ============ 表定义和元数据 ============
# 每个表定义包含：表名、表描述、字段列表（字段名、类型、描述）、外键关系

TABLE_DEFINITIONS = {
    # ============ 客户相关表 ============
    "customers": {
        "description": "客户基本信息表，存储银行/证券公司的所有客户基础资料",
        "columns": [
            ("customer_id", "INTEGER PRIMARY KEY", "客户唯一标识ID"),
            ("name", "TEXT NOT NULL", "客户姓名"),
            ("id_card", "TEXT UNIQUE", "身份证号码"),
            ("gender", "TEXT", "性别：M-男，F-女"),
            ("birth_date", "DATE", "出生日期"),
            ("phone", "TEXT", "联系电话"),
            ("email", "TEXT", "电子邮箱"),
            ("address", "TEXT", "通讯地址"),
            ("occupation", "TEXT", "职业"),
            ("annual_income", "DECIMAL(15,2)", "年收入（元）"),
            ("risk_level", "INTEGER", "风险承受等级：1-保守型，2-稳健型，3-平衡型，4-进取型，5-激进型"),
            ("customer_type", "TEXT", "客户类型：individual-个人，corporate-企业"),
            ("registration_date", "DATE", "开户日期"),
            ("status", "TEXT DEFAULT 'active'", "账户状态：active-正常，frozen-冻结，closed-注销"),
            ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "记录创建时间"),
            ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "记录更新时间"),
        ],
        "foreign_keys": [],
    },
    "customer_kyc": {
        "description": "客户KYC（了解你的客户）信息表，存储客户身份验证和合规信息",
        "columns": [
            ("kyc_id", "INTEGER PRIMARY KEY", "KYC记录ID"),
            ("customer_id", "INTEGER NOT NULL", "关联客户ID"),
            ("kyc_level", "INTEGER", "KYC认证等级：1-基础认证，2-进阶认证，3-高级认证"),
            ("verification_status", "TEXT", "验证状态：pending-待审核，approved-已通过，rejected-已拒绝"),
            ("id_verified", "BOOLEAN", "身份证是否已验证"),
            ("face_verified", "BOOLEAN", "人脸识别是否已验证"),
            ("address_verified", "BOOLEAN", "地址是否已验证"),
            ("source_of_wealth", "TEXT", "财富来源"),
            ("verification_date", "DATE", "验证日期"),
            ("expiry_date", "DATE", "验证到期日"),
            ("reviewer_id", "INTEGER", "审核人员ID"),
            ("notes", "TEXT", "备注信息"),
        ],
        "foreign_keys": [("customer_id", "customers", "customer_id")],
    },
    # ============ 账户相关表 ============
    "accounts": {
        "description": "账户信息表，记录客户开设的各类金融账户",
        "columns": [
            ("account_id", "INTEGER PRIMARY KEY", "账户唯一标识ID"),
            ("customer_id", "INTEGER NOT NULL", "账户所属客户ID"),
            ("account_number", "TEXT UNIQUE NOT NULL", "账户号码"),
            ("account_type", "TEXT NOT NULL", "账户类型：savings-储蓄，checking-活期，investment-投资，loan-贷款"),
            ("currency", "TEXT DEFAULT 'CNY'", "账户币种"),
            ("balance", "DECIMAL(18,2) DEFAULT 0", "账户余额"),
            ("available_balance", "DECIMAL(18,2) DEFAULT 0", "可用余额"),
            ("frozen_amount", "DECIMAL(18,2) DEFAULT 0", "冻结金额"),
            ("open_date", "DATE NOT NULL", "开户日期"),
            ("status", "TEXT DEFAULT 'active'", "账户状态"),
            ("branch_id", "INTEGER", "开户网点ID"),
        ],
        "foreign_keys": [("customer_id", "customers", "customer_id")],
    },
    "securities_accounts": {
        "description": "证券账户表，记录客户的股票交易账户信息",
        "columns": [
            ("sec_account_id", "INTEGER PRIMARY KEY", "证券账户ID"),
            ("customer_id", "INTEGER NOT NULL", "客户ID"),
            ("account_number", "TEXT UNIQUE", "证券账号"),
            ("account_type", "TEXT", "账户类型：A-A股账户，B-B股账户，fund-基金账户"),
            ("trading_permission", "TEXT", "交易权限：level1-普通交易，level2-融资融券，level3-期权"),
            ("margin_ratio", "DECIMAL(5,2)", "保证金比例"),
            ("total_assets", "DECIMAL(18,2)", "账户总资产"),
            ("market_value", "DECIMAL(18,2)", "证券市值"),
            ("available_funds", "DECIMAL(18,2)", "可用资金"),
            ("frozen_funds", "DECIMAL(18,2)", "冻结资金"),
            ("open_date", "DATE", "开户日期"),
            ("status", "TEXT DEFAULT 'active'", "账户状态"),
        ],
        "foreign_keys": [("customer_id", "customers", "customer_id")],
    },
    # ============ 产品相关表 ============
    "products": {
        "description": "金融产品主表，存储各类金融产品的基本信息",
        "columns": [
            ("product_id", "INTEGER PRIMARY KEY", "产品ID"),
            ("product_code", "TEXT UNIQUE NOT NULL", "产品代码"),
            ("product_name", "TEXT NOT NULL", "产品名称"),
            ("product_type", "TEXT NOT NULL", "产品类型：deposit-存款，fund-基金，insurance-保险，loan-贷款，wealth-理财"),
            ("risk_rating", "INTEGER", "风险评级：1-低风险，2-中低风险，3-中风险，4-中高风险，5-高风险"),
            ("min_investment", "DECIMAL(15,2)", "最低投资金额"),
            ("expected_return", "DECIMAL(8,4)", "预期收益率"),
            ("term_days", "INTEGER", "产品期限（天）"),
            ("status", "TEXT DEFAULT 'active'", "产品状态：active-在售，suspended-暂停，terminated-终止"),
            ("launch_date", "DATE", "上线日期"),
            ("end_date", "DATE", "终止日期"),
            ("description", "TEXT", "产品描述"),
            ("issuer", "TEXT", "发行机构"),
        ],
        "foreign_keys": [],
    },
    "fund_products": {
        "description": "基金产品详情表，存储基金类产品的详细信息",
        "columns": [
            ("fund_id", "INTEGER PRIMARY KEY", "基金ID"),
            ("product_id", "INTEGER NOT NULL", "关联产品ID"),
            ("fund_code", "TEXT UNIQUE", "基金代码"),
            ("fund_type", "TEXT", "基金类型：stock-股票型，bond-债券型，money-货币型，hybrid-混合型，index-指数型，qdii-QDII"),
            ("fund_manager", "TEXT", "基金经理"),
            ("fund_company", "TEXT", "基金公司"),
            ("nav", "DECIMAL(10,4)", "最新净值"),
            ("accumulated_nav", "DECIMAL(10,4)", "累计净值"),
            ("nav_date", "DATE", "净值日期"),
            ("fund_size", "DECIMAL(18,2)", "基金规模（亿元）"),
            ("establishment_date", "DATE", "成立日期"),
            ("benchmark", "TEXT", "业绩比较基准"),
            ("subscription_status", "TEXT", "申购状态"),
            ("redemption_status", "TEXT", "赎回状态"),
        ],
        "foreign_keys": [("product_id", "products", "product_id")],
    },
    "deposit_products": {
        "description": "存款产品详情表，存储定期存款和大额存单等产品信息",
        "columns": [
            ("deposit_id", "INTEGER PRIMARY KEY", "存款产品ID"),
            ("product_id", "INTEGER NOT NULL", "关联产品ID"),
            ("deposit_type", "TEXT", "存款类型：fixed-定期，large_cd-大额存单，structured-结构性存款"),
            ("term_months", "INTEGER", "存款期限（月）"),
            ("base_rate", "DECIMAL(6,4)", "基础利率"),
            ("float_rate", "DECIMAL(6,4)", "浮动利率上限"),
            ("min_amount", "DECIMAL(15,2)", "起存金额"),
            ("max_amount", "DECIMAL(15,2)", "最高限额"),
            ("interest_payment", "TEXT", "付息方式：maturity-到期付息，monthly-按月付息，quarterly-按季付息"),
            ("early_withdrawal_rate", "DECIMAL(6,4)", "提前支取利率"),
            ("auto_renewal", "BOOLEAN", "是否自动续存"),
        ],
        "foreign_keys": [("product_id", "products", "product_id")],
    },
    "loan_products": {
        "description": "贷款产品详情表，存储各类贷款产品的详细参数",
        "columns": [
            ("loan_product_id", "INTEGER PRIMARY KEY", "贷款产品ID"),
            ("product_id", "INTEGER NOT NULL", "关联产品ID"),
            ("loan_type", "TEXT", "贷款类型：mortgage-房贷，car-车贷，personal-个人消费贷，business-经营贷"),
            ("interest_rate_type", "TEXT", "利率类型：fixed-固定利率，floating-浮动利率，lpr-LPR加点"),
            ("base_rate", "DECIMAL(6,4)", "基准利率"),
            ("rate_spread", "DECIMAL(6,4)", "利率加点"),
            ("min_amount", "DECIMAL(15,2)", "最低贷款额"),
            ("max_amount", "DECIMAL(15,2)", "最高贷款额"),
            ("min_term_months", "INTEGER", "最短期限（月）"),
            ("max_term_months", "INTEGER", "最长期限（月）"),
            ("repayment_method", "TEXT", "还款方式：equal_principal-等额本金，equal_installment-等额本息，interest_first-先息后本"),
            ("collateral_required", "BOOLEAN", "是否需要抵押"),
            ("guarantee_required", "BOOLEAN", "是否需要担保"),
        ],
        "foreign_keys": [("product_id", "products", "product_id")],
    },
    # ============ 交易相关表 ============
    "transactions": {
        "description": "交易流水表，记录所有账户的资金交易明细",
        "columns": [
            ("transaction_id", "INTEGER PRIMARY KEY", "交易ID"),
            ("account_id", "INTEGER NOT NULL", "账户ID"),
            ("transaction_type", "TEXT NOT NULL", "交易类型：deposit-存入，withdraw-取出，transfer_in-转入，transfer_out-转出，payment-支付，fee-手续费"),
            ("amount", "DECIMAL(18,2) NOT NULL", "交易金额"),
            ("balance_after", "DECIMAL(18,2)", "交易后余额"),
            ("currency", "TEXT DEFAULT 'CNY'", "交易币种"),
            ("counterparty_account", "TEXT", "对方账户"),
            ("counterparty_name", "TEXT", "对方户名"),
            ("channel", "TEXT", "交易渠道：counter-柜台，atm-ATM，mobile-手机银行，online-网银，pos-POS"),
            ("reference_number", "TEXT", "交易参考号"),
            ("description", "TEXT", "交易描述"),
            ("status", "TEXT DEFAULT 'completed'", "交易状态：pending-处理中，completed-已完成，failed-失败，reversed-已冲正"),
            ("transaction_time", "TIMESTAMP NOT NULL", "交易时间"),
            ("posting_date", "DATE", "入账日期"),
        ],
        "foreign_keys": [("account_id", "accounts", "account_id")],
    },
    "stock_trades": {
        "description": "股票交易记录表，记录证券账户的股票买卖交易",
        "columns": [
            ("trade_id", "INTEGER PRIMARY KEY", "交易ID"),
            ("sec_account_id", "INTEGER NOT NULL", "证券账户ID"),
            ("stock_code", "TEXT NOT NULL", "股票代码"),
            ("stock_name", "TEXT", "股票名称"),
            ("trade_type", "TEXT NOT NULL", "交易类型：buy-买入，sell-卖出"),
            ("order_type", "TEXT", "订单类型：limit-限价，market-市价"),
            ("price", "DECIMAL(10,2)", "成交价格"),
            ("quantity", "INTEGER NOT NULL", "成交数量"),
            ("amount", "DECIMAL(18,2)", "成交金额"),
            ("commission", "DECIMAL(10,2)", "佣金"),
            ("stamp_duty", "DECIMAL(10,2)", "印花税"),
            ("transfer_fee", "DECIMAL(10,2)", "过户费"),
            ("trade_time", "TIMESTAMP NOT NULL", "成交时间"),
            ("settlement_date", "DATE", "交割日期"),
            ("order_id", "TEXT", "委托单号"),
            ("status", "TEXT", "交易状态"),
        ],
        "foreign_keys": [("sec_account_id", "securities_accounts", "sec_account_id")],
    },
    "fund_trades": {
        "description": "基金交易记录表，记录基金申购赎回等交易",
        "columns": [
            ("trade_id", "INTEGER PRIMARY KEY", "交易ID"),
            ("account_id", "INTEGER NOT NULL", "账户ID"),
            ("fund_id", "INTEGER NOT NULL", "基金ID"),
            ("trade_type", "TEXT NOT NULL", "交易类型：subscribe-申购，redeem-赎回，dividend-分红，conversion-转换"),
            ("amount", "DECIMAL(18,2)", "交易金额"),
            ("nav", "DECIMAL(10,4)", "成交净值"),
            ("shares", "DECIMAL(18,4)", "成交份额"),
            ("fee", "DECIMAL(10,2)", "手续费"),
            ("fee_rate", "DECIMAL(6,4)", "费率"),
            ("apply_date", "DATE", "申请日期"),
            ("confirm_date", "DATE", "确认日期"),
            ("status", "TEXT", "交易状态：pending-待确认，confirmed-已确认，cancelled-已取消"),
        ],
        "foreign_keys": [("account_id", "accounts", "account_id"), ("fund_id", "fund_products", "fund_id")],
    },
    # ============ 贷款相关表 ============
    "loan_applications": {
        "description": "贷款申请表，记录客户的贷款申请信息和审批流程",
        "columns": [
            ("application_id", "INTEGER PRIMARY KEY", "申请ID"),
            ("customer_id", "INTEGER NOT NULL", "申请客户ID"),
            ("loan_product_id", "INTEGER NOT NULL", "贷款产品ID"),
            ("apply_amount", "DECIMAL(15,2) NOT NULL", "申请金额"),
            ("apply_term", "INTEGER NOT NULL", "申请期限（月）"),
            ("purpose", "TEXT", "贷款用途"),
            ("monthly_income", "DECIMAL(15,2)", "月收入"),
            ("employment_status", "TEXT", "就业状态"),
            ("application_date", "DATE NOT NULL", "申请日期"),
            ("status", "TEXT DEFAULT 'pending'", "申请状态：pending-待审批，approved-已批准，rejected-已拒绝，cancelled-已取消"),
            ("approved_amount", "DECIMAL(15,2)", "批准金额"),
            ("approved_rate", "DECIMAL(6,4)", "批准利率"),
            ("approver_id", "INTEGER", "审批人ID"),
            ("approval_date", "DATE", "审批日期"),
            ("rejection_reason", "TEXT", "拒绝原因"),
        ],
        "foreign_keys": [("customer_id", "customers", "customer_id"), ("loan_product_id", "loan_products", "loan_product_id")],
    },
    "loans": {
        "description": "贷款合同表，记录已发放贷款的合同信息",
        "columns": [
            ("loan_id", "INTEGER PRIMARY KEY", "贷款ID"),
            ("application_id", "INTEGER NOT NULL", "关联申请ID"),
            ("customer_id", "INTEGER NOT NULL", "借款人ID"),
            ("contract_number", "TEXT UNIQUE", "合同编号"),
            ("principal", "DECIMAL(15,2) NOT NULL", "贷款本金"),
            ("interest_rate", "DECIMAL(6,4) NOT NULL", "执行利率"),
            ("term_months", "INTEGER NOT NULL", "贷款期限（月）"),
            ("start_date", "DATE NOT NULL", "起息日"),
            ("end_date", "DATE NOT NULL", "到期日"),
            ("repayment_day", "INTEGER", "还款日（每月几号）"),
            ("remaining_principal", "DECIMAL(15,2)", "剩余本金"),
            ("total_interest", "DECIMAL(15,2)", "累计应付利息"),
            ("paid_principal", "DECIMAL(15,2) DEFAULT 0", "已还本金"),
            ("paid_interest", "DECIMAL(15,2) DEFAULT 0", "已还利息"),
            ("overdue_amount", "DECIMAL(15,2) DEFAULT 0", "逾期金额"),
            ("status", "TEXT DEFAULT 'active'", "贷款状态：active-正常，overdue-逾期，settled-已结清，defaulted-违约"),
        ],
        "foreign_keys": [("application_id", "loan_applications", "application_id"), ("customer_id", "customers", "customer_id")],
    },
    "loan_repayments": {
        "description": "还款计划表，记录贷款的还款计划和实际还款情况",
        "columns": [
            ("repayment_id", "INTEGER PRIMARY KEY", "还款记录ID"),
            ("loan_id", "INTEGER NOT NULL", "贷款ID"),
            ("period_number", "INTEGER NOT NULL", "期数"),
            ("due_date", "DATE NOT NULL", "应还日期"),
            ("due_principal", "DECIMAL(15,2)", "应还本金"),
            ("due_interest", "DECIMAL(15,2)", "应还利息"),
            ("due_amount", "DECIMAL(15,2)", "应还总额"),
            ("paid_amount", "DECIMAL(15,2) DEFAULT 0", "实还金额"),
            ("paid_date", "DATE", "实还日期"),
            ("overdue_days", "INTEGER DEFAULT 0", "逾期天数"),
            ("penalty_amount", "DECIMAL(10,2) DEFAULT 0", "罚息金额"),
            ("status", "TEXT DEFAULT 'pending'", "还款状态：pending-待还，paid-已还，overdue-逾期，partial-部分还款"),
        ],
        "foreign_keys": [("loan_id", "loans", "loan_id")],
    },
    # ============ 投资持仓相关表 ============
    "stock_holdings": {
        "description": "股票持仓表，记录客户当前的股票持仓情况",
        "columns": [
            ("holding_id", "INTEGER PRIMARY KEY", "持仓ID"),
            ("sec_account_id", "INTEGER NOT NULL", "证券账户ID"),
            ("stock_code", "TEXT NOT NULL", "股票代码"),
            ("stock_name", "TEXT", "股票名称"),
            ("quantity", "INTEGER NOT NULL", "持有数量"),
            ("available_quantity", "INTEGER", "可用数量"),
            ("cost_price", "DECIMAL(10,4)", "成本价"),
            ("current_price", "DECIMAL(10,4)", "当前价"),
            ("market_value", "DECIMAL(18,2)", "市值"),
            ("profit_loss", "DECIMAL(18,2)", "盈亏金额"),
            ("profit_loss_ratio", "DECIMAL(8,4)", "盈亏比例"),
            ("update_time", "TIMESTAMP", "更新时间"),
        ],
        "foreign_keys": [("sec_account_id", "securities_accounts", "sec_account_id")],
    },
    "fund_holdings": {
        "description": "基金持仓表，记录客户当前持有的基金份额",
        "columns": [
            ("holding_id", "INTEGER PRIMARY KEY", "持仓ID"),
            ("account_id", "INTEGER NOT NULL", "账户ID"),
            ("fund_id", "INTEGER NOT NULL", "基金ID"),
            ("shares", "DECIMAL(18,4) NOT NULL", "持有份额"),
            ("available_shares", "DECIMAL(18,4)", "可用份额"),
            ("cost_amount", "DECIMAL(18,2)", "投入成本"),
            ("current_nav", "DECIMAL(10,4)", "当前净值"),
            ("market_value", "DECIMAL(18,2)", "当前市值"),
            ("profit_loss", "DECIMAL(18,2)", "盈亏金额"),
            ("dividend_amount", "DECIMAL(18,2) DEFAULT 0", "累计分红"),
            ("holding_days", "INTEGER", "持有天数"),
            ("update_date", "DATE", "更新日期"),
        ],
        "foreign_keys": [("account_id", "accounts", "account_id"), ("fund_id", "fund_products", "fund_id")],
    },
    # ============ 市场数据表 ============
    "stock_quotes": {
        "description": "股票行情表，存储股票的实时和历史行情数据",
        "columns": [
            ("quote_id", "INTEGER PRIMARY KEY", "行情ID"),
            ("stock_code", "TEXT NOT NULL", "股票代码"),
            ("stock_name", "TEXT", "股票名称"),
            ("trade_date", "DATE NOT NULL", "交易日期"),
            ("open_price", "DECIMAL(10,2)", "开盘价"),
            ("high_price", "DECIMAL(10,2)", "最高价"),
            ("low_price", "DECIMAL(10,2)", "最低价"),
            ("close_price", "DECIMAL(10,2)", "收盘价"),
            ("prev_close", "DECIMAL(10,2)", "昨收价"),
            ("change_amount", "DECIMAL(10,2)", "涨跌额"),
            ("change_percent", "DECIMAL(8,4)", "涨跌幅"),
            ("volume", "BIGINT", "成交量（股）"),
            ("turnover", "DECIMAL(18,2)", "成交额（元）"),
            ("turnover_rate", "DECIMAL(8,4)", "换手率"),
        ],
        "foreign_keys": [],
    },
    "fund_nav_history": {
        "description": "基金净值历史表，记录基金每日净值变化",
        "columns": [
            ("nav_id", "INTEGER PRIMARY KEY", "净值记录ID"),
            ("fund_id", "INTEGER NOT NULL", "基金ID"),
            ("nav_date", "DATE NOT NULL", "净值日期"),
            ("nav", "DECIMAL(10,4) NOT NULL", "单位净值"),
            ("accumulated_nav", "DECIMAL(10,4)", "累计净值"),
            ("daily_return", "DECIMAL(8,4)", "日收益率"),
            ("nav_change", "DECIMAL(10,4)", "净值变动"),
        ],
        "foreign_keys": [("fund_id", "fund_products", "fund_id")],
    },
    # ============ 风控相关表 ============
    "risk_assessments": {
        "description": "风险评估表，记录客户的风险评估结果",
        "columns": [
            ("assessment_id", "INTEGER PRIMARY KEY", "评估ID"),
            ("customer_id", "INTEGER NOT NULL", "客户ID"),
            ("assessment_date", "DATE NOT NULL", "评估日期"),
            ("questionnaire_score", "INTEGER", "问卷得分"),
            ("risk_level", "INTEGER", "风险等级：1-5"),
            ("risk_category", "TEXT", "风险类别：conservative-保守型，steady-稳健型，balanced-平衡型，growth-进取型，aggressive-激进型"),
            ("investment_horizon", "TEXT", "投资期限偏好"),
            ("loss_tolerance", "DECIMAL(5,2)", "最大可承受亏损比例"),
            ("valid_until", "DATE", "有效期至"),
            ("assessor_id", "INTEGER", "评估人员ID"),
        ],
        "foreign_keys": [("customer_id", "customers", "customer_id")],
    },
    "credit_scores": {
        "description": "信用评分表，存储客户的信用评分记录",
        "columns": [
            ("score_id", "INTEGER PRIMARY KEY", "评分ID"),
            ("customer_id", "INTEGER NOT NULL", "客户ID"),
            ("score_date", "DATE NOT NULL", "评分日期"),
            ("credit_score", "INTEGER NOT NULL", "信用评分（300-850）"),
            ("score_level", "TEXT", "评分等级：AAA-最优，AA-优秀，A-良好，BBB-中等，BB-较差，B-差，C-极差"),
            ("payment_history_score", "INTEGER", "还款历史得分"),
            ("debt_ratio_score", "INTEGER", "负债比率得分"),
            ("credit_history_score", "INTEGER", "信用历史得分"),
            ("credit_types_score", "INTEGER", "信用类型得分"),
            ("new_credit_score", "INTEGER", "新增信用得分"),
            ("score_model", "TEXT", "评分模型版本"),
            ("data_source", "TEXT", "数据来源"),
        ],
        "foreign_keys": [("customer_id", "customers", "customer_id")],
    },
    "fraud_alerts": {
        "description": "欺诈预警表，记录系统检测到的可疑交易",
        "columns": [
            ("alert_id", "INTEGER PRIMARY KEY", "预警ID"),
            ("transaction_id", "INTEGER", "关联交易ID"),
            ("customer_id", "INTEGER NOT NULL", "客户ID"),
            ("alert_type", "TEXT NOT NULL", "预警类型：unusual_amount-异常金额，unusual_location-异常地点，unusual_time-异常时间，velocity-高频交易"),
            ("risk_score", "INTEGER", "风险评分（1-100）"),
            ("alert_time", "TIMESTAMP NOT NULL", "预警时间"),
            ("description", "TEXT", "预警描述"),
            ("status", "TEXT DEFAULT 'pending'", "处理状态：pending-待处理，investigating-调查中，confirmed-已确认，false_positive-误报"),
            ("handler_id", "INTEGER", "处理人员ID"),
            ("resolution_time", "TIMESTAMP", "处理时间"),
            ("resolution_notes", "TEXT", "处理备注"),
        ],
        "foreign_keys": [("customer_id", "customers", "customer_id")],
    },
    # ============ 员工和网点表 ============
    "employees": {
        "description": "员工信息表，存储机构员工的基本信息",
        "columns": [
            ("employee_id", "INTEGER PRIMARY KEY", "员工ID"),
            ("employee_number", "TEXT UNIQUE", "员工工号"),
            ("name", "TEXT NOT NULL", "员工姓名"),
            ("department", "TEXT", "所属部门"),
            ("position", "TEXT", "职位"),
            ("branch_id", "INTEGER", "所属网点ID"),
            ("hire_date", "DATE", "入职日期"),
            ("status", "TEXT DEFAULT 'active'", "在职状态"),
            ("phone", "TEXT", "联系电话"),
            ("email", "TEXT", "电子邮箱"),
            ("manager_id", "INTEGER", "上级主管ID"),
        ],
        "foreign_keys": [],
    },
    "branches": {
        "description": "网点信息表，存储银行/证券公司各网点信息",
        "columns": [
            ("branch_id", "INTEGER PRIMARY KEY", "网点ID"),
            ("branch_code", "TEXT UNIQUE", "网点编码"),
            ("branch_name", "TEXT NOT NULL", "网点名称"),
            ("branch_type", "TEXT", "网点类型：headquarters-总部，sub_branch-分行，outlet-支行，community-社区网点"),
            ("province", "TEXT", "省份"),
            ("city", "TEXT", "城市"),
            ("district", "TEXT", "区县"),
            ("address", "TEXT", "详细地址"),
            ("phone", "TEXT", "联系电话"),
            ("manager_id", "INTEGER", "网点负责人ID"),
            ("open_date", "DATE", "开业日期"),
            ("status", "TEXT DEFAULT 'active'", "运营状态"),
        ],
        "foreign_keys": [],
    },
    # ============ 系统配置表 ============
    "exchange_rates": {
        "description": "汇率表，存储各币种之间的汇率",
        "columns": [
            ("rate_id", "INTEGER PRIMARY KEY", "汇率ID"),
            ("from_currency", "TEXT NOT NULL", "源币种"),
            ("to_currency", "TEXT NOT NULL", "目标币种"),
            ("rate_date", "DATE NOT NULL", "汇率日期"),
            ("buying_rate", "DECIMAL(12,6)", "买入汇率"),
            ("selling_rate", "DECIMAL(12,6)", "卖出汇率"),
            ("middle_rate", "DECIMAL(12,6)", "中间汇率"),
        ],
        "foreign_keys": [],
    },
    "interest_rate_table": {
        "description": "利率表，存储各类产品的利率配置",
        "columns": [
            ("rate_id", "INTEGER PRIMARY KEY", "利率ID"),
            ("rate_type", "TEXT NOT NULL", "利率类型：deposit-存款利率，loan-贷款利率，lpr-LPR"),
            ("term_months", "INTEGER", "期限（月）"),
            ("rate_value", "DECIMAL(8,4) NOT NULL", "利率值"),
            ("effective_date", "DATE NOT NULL", "生效日期"),
            ("end_date", "DATE", "失效日期"),
            ("description", "TEXT", "说明"),
        ],
        "foreign_keys": [],
    },
}


def create_tables(conn: sqlite3.Connection):
    """创建所有表"""
    cursor = conn.cursor()
    
    for table_name, table_def in TABLE_DEFINITIONS.items():
        columns_sql = []
        for col_name, col_type, col_desc in table_def["columns"]:
            columns_sql.append(f"    {col_name} {col_type}")
        
        for fk in table_def.get("foreign_keys", []):
            columns_sql.append(f"    FOREIGN KEY ({fk[0]}) REFERENCES {fk[1]}({fk[2]})")
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n{','.join(columns_sql)}\n);"
        cursor.execute(create_sql)
        
        # 添加表注释（SQLite不支持原生注释，使用sqlite_master的sql字段存储）
        # 我们通过单独的元数据表来管理
    
    # 创建元数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _table_metadata (
            table_name TEXT PRIMARY KEY,
            description TEXT
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _column_metadata (
            id INTEGER PRIMARY KEY,
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            description TEXT,
            UNIQUE(table_name, column_name)
        );
    """)
    
    conn.commit()


def insert_metadata(conn: sqlite3.Connection):
    """插入表和字段的元数据"""
    cursor = conn.cursor()
    
    for table_name, table_def in TABLE_DEFINITIONS.items():
        # 插入表描述
        cursor.execute(
            "INSERT OR REPLACE INTO _table_metadata (table_name, description) VALUES (?, ?)",
            (table_name, table_def["description"])
        )
        
        # 插入字段描述
        for col_name, col_type, col_desc in table_def["columns"]:
            cursor.execute(
                "INSERT OR REPLACE INTO _column_metadata (table_name, column_name, description) VALUES (?, ?, ?)",
                (table_name, col_name, col_desc)
            )
    
    conn.commit()


def generate_sample_data(conn: sqlite3.Connection):
    """生成样例数据"""
    cursor = conn.cursor()
    
    # ============ 生成基础数据 ============
    
    # 1. 网点数据
    branches = [
        ("BR001", "北京总部", "headquarters", "北京", "北京", "朝阳区", "朝阳区金融街1号", "010-88888888"),
        ("BR002", "上海分行", "sub_branch", "上海", "上海", "浦东新区", "浦东新区陆家嘴路100号", "021-66666666"),
        ("BR003", "广州分行", "sub_branch", "广东", "广州", "天河区", "天河区珠江新城88号", "020-55555555"),
        ("BR004", "深圳分行", "sub_branch", "广东", "深圳", "福田区", "福田区深南大道1000号", "0755-44444444"),
        ("BR005", "杭州支行", "outlet", "浙江", "杭州", "西湖区", "西湖区文三路200号", "0571-33333333"),
    ]
    for br in branches:
        cursor.execute(
            "INSERT INTO branches (branch_code, branch_name, branch_type, province, city, district, address, phone, open_date, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '2010-01-01', 'active')",
            br
        )
    
    # 2. 员工数据
    employees = [
        ("E001", "张三", "零售银行部", "客户经理", 1, "2015-03-15"),
        ("E002", "李四", "风险管理部", "风控专员", 1, "2016-07-20"),
        ("E003", "王五", "投资银行部", "投资顾问", 2, "2017-01-10"),
        ("E004", "赵六", "信贷部", "信贷审批员", 2, "2018-05-25"),
        ("E005", "钱七", "零售银行部", "大堂经理", 3, "2019-09-01"),
    ]
    for emp in employees:
        cursor.execute(
            "INSERT INTO employees (employee_number, name, department, position, branch_id, hire_date, status) VALUES (?, ?, ?, ?, ?, ?, 'active')",
            emp
        )
    
    # 3. 客户数据
    first_names = ["张", "李", "王", "刘", "陈", "杨", "赵", "黄", "周", "吴", "徐", "孙", "马", "胡", "郭", "林", "何", "高", "罗", "郑"]
    last_names = ["伟", "芳", "娜", "秀英", "敏", "静", "丽", "强", "磊", "军", "洋", "勇", "艳", "杰", "涛", "明", "超", "秀兰", "霞", "平"]
    occupations = ["软件工程师", "医生", "教师", "会计", "销售经理", "公务员", "自由职业", "企业主", "金融分析师", "律师"]
    
    customers = []
    for i in range(100):
        name = random.choice(first_names) + random.choice(last_names)
        gender = random.choice(["M", "F"])
        birth_year = random.randint(1960, 2000)
        birth_date = f"{birth_year}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        phone = f"1{random.choice(['38','39','58','59','36','37'])}{random.randint(10000000,99999999)}"
        income = random.randint(50000, 2000000)
        risk_level = random.randint(1, 5)
        reg_date = f"20{random.randint(15,23)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        
        cursor.execute(
            """INSERT INTO customers (name, gender, birth_date, phone, occupation, annual_income, risk_level, customer_type, registration_date, status) 
               VALUES (?, ?, ?, ?, ?, ?, ?, 'individual', ?, 'active')""",
            (name, gender, birth_date, phone, random.choice(occupations), income, risk_level, reg_date)
        )
        customers.append(cursor.lastrowid)
    
    # 4. KYC数据
    for cid in customers[:80]:
        cursor.execute(
            """INSERT INTO customer_kyc (customer_id, kyc_level, verification_status, id_verified, face_verified, address_verified, verification_date)
               VALUES (?, ?, 'approved', 1, 1, 1, date('now', '-' || ? || ' days'))""",
            (cid, random.randint(1, 3), random.randint(30, 365))
        )
    
    # 5. 账户数据
    accounts = []
    for cid in customers:
        account_num = f"6222{random.randint(1000000000000000, 9999999999999999)}"
        account_type = random.choice(["savings", "checking", "investment"])
        balance = random.uniform(1000, 5000000)
        open_date = f"20{random.randint(15,23)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        cursor.execute(
            """INSERT INTO accounts (customer_id, account_number, account_type, balance, available_balance, open_date, status, branch_id)
               VALUES (?, ?, ?, ?, ?, ?, 'active', ?)""",
            (cid, account_num, account_type, balance, balance, open_date, random.randint(1, 5))
        )
        accounts.append(cursor.lastrowid)
    
    # 6. 证券账户
    sec_accounts = []
    for cid in random.sample(customers, 60):
        sec_num = f"A{random.randint(100000000, 999999999)}"
        cursor.execute(
            """INSERT INTO securities_accounts (customer_id, account_number, account_type, trading_permission, total_assets, available_funds, open_date, status)
               VALUES (?, ?, 'A', ?, ?, ?, date('now', '-' || ? || ' days'), 'active')""",
            (cid, sec_num, random.choice(["level1", "level2"]), random.uniform(10000, 2000000), random.uniform(1000, 100000), random.randint(30, 1000))
        )
        sec_accounts.append(cursor.lastrowid)
    
    # 7. 金融产品
    products = []
    
    # 存款产品
    deposit_products = [
        ("DEP001", "活期存款", "deposit", 1, 0, 0.0035, 0),
        ("DEP002", "三个月定期", "deposit", 1, 1000, 0.0155, 90),
        ("DEP003", "六个月定期", "deposit", 1, 1000, 0.0175, 180),
        ("DEP004", "一年定期", "deposit", 1, 1000, 0.0195, 365),
        ("DEP005", "大额存单", "deposit", 1, 200000, 0.0280, 365),
    ]
    for p in deposit_products:
        cursor.execute(
            """INSERT INTO products (product_code, product_name, product_type, risk_rating, min_investment, expected_return, term_days, status, launch_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'active', '2020-01-01')""",
            p
        )
        products.append(cursor.lastrowid)
    
    # 基金产品
    fund_products_data = [
        ("F001", "稳健增长混合A", "fund", 3, 100, 0.0650, 0, "hybrid", "张明", "华夏基金"),
        ("F002", "蓝筹精选股票", "fund", 4, 100, 0.0850, 0, "stock", "李强", "易方达基金"),
        ("F003", "纯债增利A", "fund", 2, 100, 0.0420, 0, "bond", "王芳", "南方基金"),
        ("F004", "货币市场A", "fund", 1, 1, 0.0220, 0, "money", "赵伟", "天弘基金"),
        ("F005", "沪深300指数", "fund", 3, 10, 0.0780, 0, "index", "刘涛", "嘉实基金"),
        ("F006", "创业板ETF联接", "fund", 4, 10, 0.0920, 0, "index", "陈静", "广发基金"),
        ("F007", "消费行业精选", "fund", 4, 100, 0.0880, 0, "stock", "杨磊", "招商基金"),
        ("F008", "新能源主题", "fund", 5, 100, 0.1200, 0, "stock", "周杰", "汇添富基金"),
        ("F009", "医药健康混合", "fund", 4, 100, 0.0950, 0, "hybrid", "吴敏", "工银瑞信"),
        ("F010", "科技创新混合", "fund", 4, 100, 0.1050, 0, "hybrid", "孙俊", "中欧基金"),
    ]
    fund_ids = []
    for f in fund_products_data:
        cursor.execute(
            """INSERT INTO products (product_code, product_name, product_type, risk_rating, min_investment, expected_return, term_days, status, launch_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'active', '2020-01-01')""",
            f[:7]
        )
        pid = cursor.lastrowid
        products.append(pid)
        
        nav = round(random.uniform(0.8, 3.5), 4)
        cursor.execute(
            """INSERT INTO fund_products (product_id, fund_code, fund_type, fund_manager, fund_company, nav, accumulated_nav, nav_date, fund_size, establishment_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, date('now'), ?, '2018-01-01')""",
            (pid, f[0], f[7], f[8], f[9], nav, nav + random.uniform(0, 1), random.uniform(10, 500))
        )
        fund_ids.append(cursor.lastrowid)
    
    # 贷款产品
    loan_products_data = [
        ("LOAN001", "个人住房贷款", "loan", 2, 100000, 0.0410, 360, "mortgage"),
        ("LOAN002", "个人消费贷款", "loan", 3, 10000, 0.0650, 36, "personal"),
        ("LOAN003", "汽车贷款", "loan", 2, 50000, 0.0520, 60, "car"),
        ("LOAN004", "小微企业贷款", "loan", 3, 100000, 0.0580, 24, "business"),
        ("LOAN005", "信用贷款", "loan", 3, 5000, 0.0720, 12, "personal"),
    ]
    loan_pids = []
    for l in loan_products_data:
        cursor.execute(
            """INSERT INTO products (product_code, product_name, product_type, risk_rating, min_investment, expected_return, term_days, status, launch_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'active', '2020-01-01')""",
            l[:7]
        )
        pid = cursor.lastrowid
        products.append(pid)
        
        cursor.execute(
            """INSERT INTO loan_products (product_id, loan_type, interest_rate_type, base_rate, min_amount, max_amount, min_term_months, max_term_months, repayment_method)
               VALUES (?, ?, 'lpr', ?, ?, ?, 6, ?, 'equal_installment')""",
            (pid, l[7], l[5], l[4], l[4] * 100, l[6])
        )
        loan_pids.append(cursor.lastrowid)
    
    # 8. 交易数据
    for _ in range(500):
        aid = random.choice(accounts)
        trans_type = random.choice(["deposit", "withdraw", "transfer_in", "transfer_out", "payment"])
        amount = round(random.uniform(100, 50000), 2)
        trans_time = datetime.now() - timedelta(days=random.randint(1, 365))
        cursor.execute(
            """INSERT INTO transactions (account_id, transaction_type, amount, currency, channel, status, transaction_time, posting_date)
               VALUES (?, ?, ?, 'CNY', ?, 'completed', ?, date(?))""",
            (aid, trans_type, amount, random.choice(["mobile", "online", "atm", "counter"]), trans_time, trans_time.strftime("%Y-%m-%d"))
        )
    
    # 9. 股票交易
    stocks = [
        ("600519", "贵州茅台"), ("601318", "中国平安"), ("600036", "招商银行"), ("000858", "五粮液"), ("601166", "兴业银行"),
        ("600276", "恒瑞医药"), ("000333", "美的集团"), ("600887", "伊利股份"), ("601888", "中国中免"), ("000001", "平安银行"),
    ]
    for _ in range(300):
        said = random.choice(sec_accounts)
        stock = random.choice(stocks)
        trade_type = random.choice(["buy", "sell"])
        price = round(random.uniform(10, 2000), 2)
        quantity = random.randint(1, 100) * 100
        cursor.execute(
            """INSERT INTO stock_trades (sec_account_id, stock_code, stock_name, trade_type, order_type, price, quantity, amount, commission, trade_time, status)
               VALUES (?, ?, ?, ?, 'limit', ?, ?, ?, ?, datetime('now', '-' || ? || ' days'), 'completed')""",
            (said, stock[0], stock[1], trade_type, price, quantity, price * quantity, round(price * quantity * 0.0003, 2), random.randint(1, 365))
        )
    
    # 10. 股票持仓
    for said in sec_accounts:
        for stock in random.sample(stocks, random.randint(1, 5)):
            quantity = random.randint(1, 50) * 100
            cost = round(random.uniform(10, 500), 2)
            current = round(cost * random.uniform(0.7, 1.5), 2)
            cursor.execute(
                """INSERT INTO stock_holdings (sec_account_id, stock_code, stock_name, quantity, available_quantity, cost_price, current_price, market_value, profit_loss)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (said, stock[0], stock[1], quantity, quantity, cost, current, quantity * current, quantity * (current - cost))
            )
    
    # 11. 基金交易和持仓
    for aid in random.sample(accounts, 50):
        fid = random.choice(fund_ids)
        amount = round(random.uniform(1000, 100000), 2)
        cursor.execute(
            """INSERT INTO fund_trades (account_id, fund_id, trade_type, amount, shares, fee, apply_date, confirm_date, status)
               VALUES (?, ?, 'subscribe', ?, ?, ?, date('now', '-' || ? || ' days'), date('now', '-' || ? || ' days'), 'confirmed')""",
            (aid, fid, amount, round(amount / random.uniform(1, 3), 4), round(amount * 0.0015, 2), random.randint(30, 365), random.randint(28, 363))
        )
        
        shares = round(random.uniform(1000, 50000), 4)
        cursor.execute(
            """INSERT INTO fund_holdings (account_id, fund_id, shares, available_shares, cost_amount, current_nav, market_value, update_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, date('now'))""",
            (aid, fid, shares, shares, round(shares * random.uniform(1, 2), 2), round(random.uniform(1, 3), 4), round(shares * random.uniform(1, 3), 2))
        )
    
    # 12. 贷款申请和合同
    for _ in range(30):
        cid = random.choice(customers)
        lpid = random.choice(loan_pids)
        amount = round(random.uniform(50000, 1000000), 2)
        term = random.choice([12, 24, 36, 60, 120, 240, 360])
        status = random.choice(["approved", "approved", "approved", "pending", "rejected"])
        cursor.execute(
            """INSERT INTO loan_applications (customer_id, loan_product_id, apply_amount, apply_term, purpose, application_date, status, approved_amount, approved_rate)
               VALUES (?, ?, ?, ?, ?, date('now', '-' || ? || ' days'), ?, ?, ?)""",
            (cid, lpid, amount, term, random.choice(["购房", "购车", "装修", "消费", "经营"]), random.randint(30, 365), status, 
             amount if status == "approved" else None, random.uniform(0.04, 0.08) if status == "approved" else None)
        )
        
        if status == "approved":
            app_id = cursor.lastrowid
            start_date = datetime.now() - timedelta(days=random.randint(30, 300))
            cursor.execute(
                """INSERT INTO loans (application_id, customer_id, contract_number, principal, interest_rate, term_months, start_date, end_date, remaining_principal, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')""",
                (app_id, cid, f"LOAN{random.randint(100000, 999999)}", amount, random.uniform(0.04, 0.08), term, 
                 start_date.strftime("%Y-%m-%d"), (start_date + timedelta(days=term * 30)).strftime("%Y-%m-%d"), amount * random.uniform(0.5, 1.0))
            )
            
            loan_id = cursor.lastrowid
            for period in range(1, min(term, 12) + 1):
                due_date = start_date + timedelta(days=period * 30)
                due_principal = round(amount / term, 2)
                due_interest = round(amount * 0.05 / 12, 2)
                cursor.execute(
                    """INSERT INTO loan_repayments (loan_id, period_number, due_date, due_principal, due_interest, due_amount, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (loan_id, period, due_date.strftime("%Y-%m-%d"), due_principal, due_interest, due_principal + due_interest, 
                     "paid" if due_date < datetime.now() else "pending")
                )
    
    # 13. 风险评估
    for cid in random.sample(customers, 70):
        cursor.execute(
            """INSERT INTO risk_assessments (customer_id, assessment_date, questionnaire_score, risk_level, risk_category, valid_until)
               VALUES (?, date('now', '-' || ? || ' days'), ?, ?, ?, date('now', '+' || ? || ' days'))""",
            (cid, random.randint(1, 365), random.randint(20, 100), random.randint(1, 5), 
             random.choice(["conservative", "steady", "balanced", "growth", "aggressive"]), random.randint(30, 365))
        )
    
    # 14. 信用评分
    for cid in random.sample(customers, 80):
        score = random.randint(550, 850)
        level = "AAA" if score >= 800 else "AA" if score >= 750 else "A" if score >= 700 else "BBB" if score >= 650 else "BB" if score >= 600 else "B"
        cursor.execute(
            """INSERT INTO credit_scores (customer_id, score_date, credit_score, score_level, payment_history_score, debt_ratio_score)
               VALUES (?, date('now', '-' || ? || ' days'), ?, ?, ?, ?)""",
            (cid, random.randint(1, 90), score, level, random.randint(60, 100), random.randint(60, 100))
        )
    
    # 15. 欺诈预警
    for _ in range(20):
        cursor.execute(
            """INSERT INTO fraud_alerts (customer_id, alert_type, risk_score, alert_time, description, status)
               VALUES (?, ?, ?, datetime('now', '-' || ? || ' hours'), ?, ?)""",
            (random.choice(customers), random.choice(["unusual_amount", "unusual_location", "unusual_time", "velocity"]),
             random.randint(50, 100), random.randint(1, 720), "系统自动检测到异常交易行为", 
             random.choice(["pending", "investigating", "confirmed", "false_positive"]))
        )
    
    # 16. 股票行情
    for stock in stocks:
        for i in range(30):
            trade_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            close = round(random.uniform(10, 2000), 2)
            cursor.execute(
                """INSERT INTO stock_quotes (stock_code, stock_name, trade_date, open_price, high_price, low_price, close_price, prev_close, volume, turnover)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (stock[0], stock[1], trade_date, round(close * 0.99, 2), round(close * 1.05, 2), round(close * 0.95, 2), close,
                 round(close * random.uniform(0.95, 1.05), 2), random.randint(1000000, 100000000), round(random.uniform(100000000, 10000000000), 2))
            )
    
    # 17. 基金净值历史
    for fid in fund_ids:
        for i in range(30):
            nav_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            nav = round(random.uniform(0.8, 3.5), 4)
            cursor.execute(
                """INSERT INTO fund_nav_history (fund_id, nav_date, nav, accumulated_nav, daily_return)
                   VALUES (?, ?, ?, ?, ?)""",
                (fid, nav_date, nav, round(nav + random.uniform(0, 1), 4), round(random.uniform(-0.05, 0.05), 4))
            )
    
    # 18. 汇率数据
    currencies = [("USD", "CNY"), ("EUR", "CNY"), ("GBP", "CNY"), ("JPY", "CNY"), ("HKD", "CNY")]
    for curr in currencies:
        for i in range(30):
            rate_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            base_rate = {"USD": 7.2, "EUR": 7.8, "GBP": 9.1, "JPY": 0.048, "HKD": 0.92}[curr[0]]
            rate = round(base_rate * random.uniform(0.98, 1.02), 6)
            cursor.execute(
                """INSERT INTO exchange_rates (from_currency, to_currency, rate_date, buying_rate, selling_rate, middle_rate)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (curr[0], curr[1], rate_date, round(rate * 0.99, 6), round(rate * 1.01, 6), rate)
            )
    
    # 19. 利率表
    rates = [
        ("deposit", 0, 0.0035), ("deposit", 3, 0.0155), ("deposit", 6, 0.0175), ("deposit", 12, 0.0195), ("deposit", 24, 0.0240), ("deposit", 36, 0.0275),
        ("loan", 12, 0.0435), ("loan", 60, 0.0475), ("loan", 360, 0.0490),
        ("lpr", 12, 0.0345), ("lpr", 60, 0.0395),
    ]
    for r in rates:
        cursor.execute(
            """INSERT INTO interest_rate_table (rate_type, term_months, rate_value, effective_date, description)
               VALUES (?, ?, ?, '2024-01-01', ?)""",
            (r[0], r[1], r[2], f"{r[0]}利率-{r[1]}个月")
        )
    
    conn.commit()


def init_database():
    """初始化数据库"""
    # 删除旧数据库
    if DB_PATH.exists():
        DB_PATH.unlink()
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        print("创建表结构...")
        create_tables(conn)
        
        print("插入元数据...")
        insert_metadata(conn)
        
        print("生成样例数据...")
        generate_sample_data(conn)
        
        print(f"数据库初始化完成！路径: {DB_PATH}")
        
        # 统计各表数据量
        cursor = conn.cursor()
        print("\n各表数据统计:")
        for table_name in TABLE_DEFINITIONS.keys():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  {table_name}: {count} 条记录")
        
    finally:
        conn.close()


if __name__ == "__main__":
    init_database()
