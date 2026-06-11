"""SEC XBRL curated 概念集。

只摄取这份白名单内的概念（us-gaap 全集 500+/公司，全收会膨胀到不可维护）。
按"价值/质量/盈利因子所需的最小完备集"挑选；后续扩充概念后重跑摄取即可，
upsert 幂等。概念名一律用 SEC 官方驼峰原文。
"""
from __future__ import annotations

CURATED_CONCEPTS: dict[str, set[str]] = {
    "us-gaap": {
        # --- 利润表 ---
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",  # 旧 taxonomy（2018 前)
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "GrossProfit",
        "ResearchAndDevelopmentExpense",
        "SellingGeneralAndAdministrativeExpense",
        "OperatingExpenses",
        "OperatingIncomeLoss",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeTaxExpenseBenefit",
        "NetIncomeLoss",
        "EarningsPerShareBasic",
        "EarningsPerShareDiluted",
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        # --- 资产负债表 ---
        "Assets",
        "AssetsCurrent",
        "CashAndCashEquivalentsAtCarryingValue",
        "ShortTermInvestments",
        "AccountsReceivableNetCurrent",
        "InventoryNet",
        "Liabilities",
        "LiabilitiesCurrent",
        "DebtCurrent",
        "ShortTermBorrowings",
        "LongTermDebtCurrent",
        "LongTermDebt",
        "LongTermDebtNoncurrent",
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "CommonStockSharesOutstanding",
        "CommonStockSharesIssued",
        # --- 现金流量表 ---
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInInvestingActivities",
        "NetCashProvidedByUsedInFinancingActivities",
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
        "PaymentsOfDividends",
        "PaymentsOfDividendsCommonStock",
        "PaymentsForRepurchaseOfCommonStock",
    },
    "dei": {
        "EntityCommonStockSharesOutstanding",
        "EntityPublicFloat",
    },
}
