# 中国股票市场概览数据分析

## 项目描述
本项目自动采集中国股票市场（沪深北交易所）的概览数据，包括各交易所上市公司数量、板块分布、行业分布等，并进行统计分析与可视化呈现。

## 数据来源
- **上海证券交易所** (http://www.sse.com.cn/)
- **深圳证券交易所** (http://www.szse.cn/) [citation:2][citation:6]
- **北京证券交易所** (https://www.bse.cn/)
- **中国上市公司协会** [citation:3][citation:9][citation:10]
- **国家统计局** (历史年度数据)

## 数据时间范围
- 年度总数：1990-2026年
- 最新行业分布：2026年1-3月 [citation:1][citation:2][citation:3]

## 环境要求
- Python 3.8+
- 依赖库：pandas, numpy, requests, beautifulsoup4, matplotlib, seaborn, openpyxl

## 快速开始
1. 安装依赖：`pip install -r requirements.txt`
2. 运行Jupyter notebook：`jupyter notebook China_stock_analysis.ipynb`
3. 按顺序执行所有单元格

## 输出结果
- `output/figures/` - 保存所有可视化图表
- `output/reports/` - 保存统计分析报告（Excel格式）
- `data/` - 保存原始采集数据

## 注意事项
- 部分网站有反爬机制，如遇请求失败请适当增加延时
- 数据仅供参考，不构成投资建议
- 如需最新实时数据，建议直接访问各交易所官网