## 班级：25PA
## 组别：第八组
## 组长：于瀚洋25210288
## 组员：张倩莹25210301，陈泽鸿25210118，田云华25210239，魏小朵25210253，周翔25210313
# 中国股票市场概览数据分析

## 项目简介

本项目对中国三大证券交易所（上交所、深交所、北交所）的上市公司数量进行系统性统计分析，涵盖历史年度趋势（1990–2026）、各板块结构分布及最新行业分布，并生成可视化图表与结构化报告。

## 数据来源

| 数据类型 | 来源 |
|---|---|
| 上交所年度数据 | 上海证券交易所官网统计年鉴 |
| 深交所年度数据 | 深圳证券交易所官网市场数据 |
| 北交所数据 | 北京证券交易所官网（2021年起） |
| 行业分类 | 中国证监会行业分类标准（2012版） |
| 综合整理 | 中国上市公司协会、Wind金融终端 |

> **说明**：由于网络环境限制，本项目数据以权威公开数据为基础进行整理内嵌（非实时爬取）。如需接入实时数据，可在网络畅通的环境下将数据采集模块替换为 `akshare` 或 `tushare` 调用。

## 目录结构

```
china_stock_analysis/
├── readme.md                          # 项目说明文档
├── China_stock_analysis.ipynb         # 主分析 Notebook
├── data/
│   ├── exchange_overview.csv          # 各交易所最新概况
│   ├── annual_totals.csv              # 年度历史数据
│   └── industry_distribution.csv     # 行业分布数据
└── output/
    ├── figures/
    │   ├── 01_annual_trend.png        # 年度趋势图
    │   ├── 02_exchange_stacked.png    # 各交所分板块堆叠图
    │   ├── 03_exchange_pie.png        # 交易所占比饼图
    │   ├── 04_industry_sse.png        # 上交所行业分布
    │   ├── 05_industry_szse.png       # 深交所行业分布
    │   ├── 06_industry_bse.png        # 北交所行业分布
    │   └── 07_growth_analysis.png     # 增长率分析
    └── reports/
        └── stock_market_report.xlsx   # Excel 综合报告
```

## 运行环境

- Python 3.8+
- 依赖库：`pandas`, `numpy`, `matplotlib`, `seaborn`, `openpyxl`

```bash
pip install pandas numpy matplotlib seaborn openpyxl
```

## 运行方式

```bash
# 启动 Jupyter Notebook
jupyter notebook China_stock_analysis.ipynb

# 或使用 JupyterLab
jupyter lab China_stock_analysis.ipynb
```

依次执行所有单元格，分析结果将自动保存至 `output/` 目录。

## 主要分析内容

1. **年度趋势分析**：1990–2026 年三大交易所上市公司总数变化
2. **板块结构分析**：各交易所主板/科创板/创业板/中小板构成
3. **增长率分析**：年度环比增长与阶段性高速增长识别
4. **行业分布分析**：最新年度（2026年1月）三大交易所行业分布对比
5. **综合统计摘要**：关键指标汇总与解读

## 数据说明

- **中小板**：深交所中小板已于 2021 年 6 月并入主板，2021 年前单独统计
- **北交所**：2021 年 11 月正式开市，数据从 2021 年起计
- **2026 年数据**：截至 2026 年 3 月，为最新可用数据
