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
        # 营收瀑布链补全（XBRL US fundamental accounting concepts 的标准营收族：
        # 旧 taxonomy 商品/服务拆分 + 金融/行业总营收概念，银行保险券商不报 Revenues）
        "SalesRevenueGoodsNet",
        "SalesRevenueServicesNet",
        "RevenuesNetOfInterestExpense",
        "RegulatedAndUnregulatedOperatingRevenue",
        "InterestAndDividendIncomeOperating",
        "HealthCareOrganizationRevenue",
        "RealEstateRevenueNet",
        "OilAndGasRevenue",
        "FinancialServicesRevenue",
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
    # IFRS 申报方（20-F/40-F 的 ADR：TSM、BABA 等）在 companyfacts 里挂 "ifrs-full"
    # taxonomy；概念名与 us-gaap 块经济口径一一对应（IFRS 官方元素名，驼峰原文）。
    "ifrs-full": {
        # --- 利润表 ---
        "Revenue",
        "RevenueFromContractsWithCustomers",  # IFRS 15
        "CostOfSales",
        "GrossProfit",
        "ResearchAndDevelopmentExpense",
        "SellingGeneralAndAdministrativeExpense",
        "DistributionCosts",  # IAS 1 按功能法拆分（部分申报方不报 SG&A 合计）
        "AdministrativeExpense",
        "ProfitLossFromOperatingActivities",
        "ProfitLossBeforeTax",
        "IncomeTaxExpenseContinuingOperations",
        "ProfitLoss",
        "ProfitLossAttributableToOwnersOfParent",
        "BasicEarningsLossPerShare",
        "DilutedEarningsLossPerShare",
        "WeightedAverageShares",  # basic 加权股数
        "AdjustedWeightedAverageShares",  # diluted 加权股数
        # --- 资产负债表 ---
        "Assets",
        "CurrentAssets",
        "CashAndCashEquivalents",
        "TradeAndOtherCurrentReceivables",
        "Inventories",
        "Liabilities",
        "CurrentLiabilities",
        "Borrowings",
        "ShorttermBorrowings",
        "LongtermBorrowings",
        "Equity",
        "EquityAttributableToOwnersOfParent",
        "NumberOfSharesIssued",
        "NumberOfSharesOutstanding",
        # --- 现金流量表 ---
        "CashFlowsFromUsedInOperatingActivities",
        "CashFlowsFromUsedInInvestingActivities",
        "CashFlowsFromUsedInFinancingActivities",
        "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
        "DepreciationAndAmortisationExpense",
        "DividendsPaidClassifiedAsFinancingActivities",
        "DividendsPaidToEquityHoldersOfParentClassifiedAsFinancingActivities",
        "PaymentsToAcquireOrRedeemEntitysShares",
    },
}
