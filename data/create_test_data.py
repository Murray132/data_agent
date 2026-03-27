# -*- coding: utf-8 -*-
"""
创建测试数据 - 无元数据的表和字段

用于测试元数据补全Agent功能
"""

import sqlite3
import os
from pathlib import Path

# 数据库路径
DB_PATH = Path(__file__).parent / "finance.db"


def create_test_tables_without_metadata():
    """
    创建几张没有元数据描述的测试表
    """
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # 1. 创建投资组合表（无元数据）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolios (
            portfolio_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            portfolio_name TEXT NOT NULL,
            portfolio_type TEXT,
            risk_level TEXT,
            target_return DECIMAL(8,4),
            current_value DECIMAL(18,2),
            invested_amount DECIMAL(18,2),
            profit_loss DECIMAL(18,2),
            create_date DATE,
            last_rebalance_date DATE,
            status TEXT DEFAULT 'active',
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    # 2. 创建资产配置记录表（无元数据）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS asset_allocations (
            allocation_id INTEGER PRIMARY KEY,
            portfolio_id INTEGER,
            asset_class TEXT,
            target_weight DECIMAL(5,2),
            current_weight DECIMAL(5,2),
            market_value DECIMAL(18,2),
            update_time DATETIME,
            FOREIGN KEY (portfolio_id) REFERENCES portfolios(portfolio_id)
        )
    """)
    
    # 3. 创建客户反馈表（无元数据）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_feedback (
            feedback_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            feedback_type TEXT,
            subject TEXT,
            content TEXT,
            rating INTEGER,
            channel TEXT,
            handler_id INTEGER,
            status TEXT,
            create_time DATETIME,
            resolve_time DATETIME,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (handler_id) REFERENCES employees(employee_id)
        )
    """)
    
    # 插入测试数据
    # 投资组合数据
    portfolios_data = [
        (1, '稳健增值组合', 'balanced', 'medium', 0.08, 1500000.00, 1200000.00, 300000.00, '2023-01-15', '2024-06-01', 'active'),
        (2, '激进成长组合', 'growth', 'high', 0.15, 800000.00, 600000.00, 200000.00, '2023-03-20', '2024-05-15', 'active'),
        (3, '保守收益组合', 'conservative', 'low', 0.04, 2000000.00, 1900000.00, 100000.00, '2022-06-10', '2024-04-01', 'active'),
        (5, '价值投资组合', 'value', 'medium', 0.10, 500000.00, 450000.00, 50000.00, '2023-08-01', '2024-03-20', 'active'),
        (8, '科技主题组合', 'thematic', 'high', 0.20, 300000.00, 350000.00, -50000.00, '2024-01-10', None, 'active'),
    ]
    
    cursor.executemany("""
        INSERT OR IGNORE INTO portfolios 
        (customer_id, portfolio_name, portfolio_type, risk_level, target_return, 
         current_value, invested_amount, profit_loss, create_date, last_rebalance_date, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, portfolios_data)
    
    # 资产配置数据
    allocations_data = [
        (1, 'equity', 40.00, 42.50, 637500.00, '2024-06-01 10:00:00'),
        (1, 'fixed_income', 35.00, 33.00, 495000.00, '2024-06-01 10:00:00'),
        (1, 'cash', 15.00, 14.50, 217500.00, '2024-06-01 10:00:00'),
        (1, 'alternatives', 10.00, 10.00, 150000.00, '2024-06-01 10:00:00'),
        (2, 'equity', 70.00, 72.00, 576000.00, '2024-05-15 09:30:00'),
        (2, 'fixed_income', 20.00, 18.00, 144000.00, '2024-05-15 09:30:00'),
        (2, 'cash', 10.00, 10.00, 80000.00, '2024-05-15 09:30:00'),
    ]
    
    cursor.executemany("""
        INSERT OR IGNORE INTO asset_allocations 
        (portfolio_id, asset_class, target_weight, current_weight, market_value, update_time)
        VALUES (?, ?, ?, ?, ?, ?)
    """, allocations_data)
    
    # 客户反馈数据
    feedback_data = [
        (1, 'complaint', 'APP登录问题', '手机APP经常登录超时，希望优化', 2, 'app', 1, 'resolved', '2024-05-01 14:30:00', '2024-05-02 10:00:00'),
        (3, 'suggestion', '增加定投功能', '希望增加基金定投的自动扣款功能', 4, 'website', 2, 'pending', '2024-05-10 09:15:00', None),
        (5, 'inquiry', '账户安全咨询', '想了解账户的安全保护措施', 5, 'phone', 3, 'resolved', '2024-05-15 11:00:00', '2024-05-15 11:30:00'),
        (8, 'complaint', '交易延迟', '股票买入确认太慢', 3, 'app', 1, 'in_progress', '2024-06-01 15:45:00', None),
        (10, 'praise', '服务满意', '客户经理服务态度很好，专业负责', 5, 'email', None, 'closed', '2024-06-05 16:20:00', '2024-06-05 16:20:00'),
    ]
    
    cursor.executemany("""
        INSERT OR IGNORE INTO customer_feedback 
        (customer_id, feedback_type, subject, content, rating, channel, handler_id, status, create_time, resolve_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, feedback_data)
    
    conn.commit()
    print("✅ 已创建3张无元数据的测试表:")
    print("   - portfolios (投资组合表)")
    print("   - asset_allocations (资产配置表)")  
    print("   - customer_feedback (客户反馈表)")
    
    conn.close()


def remove_some_metadata():
    """
    删除一些现有字段的元数据描述，用于测试
    """
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # 删除accounts表部分字段的描述
    cursor.execute("""
        DELETE FROM _column_metadata 
        WHERE table_name = 'accounts' 
        AND column_name IN ('frozen_amount', 'available_balance', 'open_date')
    """)
    
    # 删除transactions表部分字段的描述
    cursor.execute("""
        DELETE FROM _column_metadata 
        WHERE table_name = 'transactions' 
        AND column_name IN ('channel', 'reference_no', 'status')
    """)
    
    conn.commit()
    
    deleted_count = cursor.rowcount
    print(f"✅ 已删除 {deleted_count} 个字段的元数据描述")
    print("   - accounts表: frozen_amount, available_balance, open_date")
    print("   - transactions表: channel, reference_no, status")
    
    conn.close()


if __name__ == "__main__":
    print("=" * 50)
    print("创建测试数据 - 元数据补全Agent测试用")
    print("=" * 50)
    
    if not DB_PATH.exists():
        print(f"❌ 数据库文件不存在: {DB_PATH}")
        exit(1)
    
    create_test_tables_without_metadata()
    print()
    remove_some_metadata()
    
    print()
    print("=" * 50)
    print("测试数据创建完成！")
    print("现在可以测试元数据补全Agent了")
    print("=" * 50)
