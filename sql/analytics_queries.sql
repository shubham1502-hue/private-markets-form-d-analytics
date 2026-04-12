-- ============================================================
-- analytics_queries.sql
-- Private Markets Fundraising Intelligence
-- SEC Form D Analytics — MySQL Query Library
-- Database: form_d_analytics | Table: form_d
-- ============================================================

USE form_d_analytics;


-- ── 1. QUARTERLY FUNDRAISING TRENDS ─────────────────────────────────────────
SELECT
    filing_quarter,
    COUNT(*)                                                AS total_filings,
    ROUND(SUM(total_offering_amount) / 1e9, 2)             AS capital_raised_bn,
    ROUND(AVG(total_offering_amount) / 1e6, 2)             AS avg_fund_size_mn,
    ROUND(SUM(amount_sold) / 1e9, 2)                       AS capital_closed_bn,
    ROUND(
        SUM(amount_sold) / NULLIF(SUM(total_offering_amount), 0) * 100, 2
    )                                                       AS close_rate_pct
FROM form_d
WHERE quality_tier = 'Clean'
  AND filing_quarter IS NOT NULL
GROUP BY filing_quarter
ORDER BY filing_quarter;


-- ── 2. ASSET CLASS CAPITAL BREAKDOWN ─────────────────────────────────────────
SELECT
    asset_class_group,
    fund_type,
    COUNT(*)                                                AS fund_count,
    ROUND(SUM(total_offering_amount) / 1e9, 2)             AS total_capital_bn,
    ROUND(AVG(total_offering_amount) / 1e6, 2)             AS avg_fund_size_mn,
    ROUND(
        SUM(total_offering_amount) * 100.0 /
        (SELECT SUM(total_offering_amount)
         FROM form_d WHERE quality_tier = 'Clean'), 2
    )                                                       AS pct_of_total_capital
FROM form_d
WHERE quality_tier = 'Clean'
GROUP BY asset_class_group, fund_type
ORDER BY total_capital_bn DESC;


-- ── 3. FEDERAL EXEMPTION BREAKDOWN ───────────────────────────────────────────
-- 506(b) vs 506(c): key signal in private markets research
SELECT
    federal_exemption,
    COUNT(*)                                                AS filing_count,
    ROUND(SUM(total_offering_amount) / 1e9, 2)             AS capital_raised_bn,
    ROUND(AVG(total_offering_amount) / 1e6, 2)             AS avg_offering_mn,
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM form_d WHERE quality_tier = 'Clean'), 2)
                                                            AS share_of_filings_pct
FROM form_d
WHERE quality_tier = 'Clean'
GROUP BY federal_exemption
ORDER BY filing_count DESC;


-- ── 4. FUND SIZE PARETO ANALYSIS ─────────────────────────────────────────────
-- Mega funds = ~8% of filings, ~78% of capital
SELECT
    fund_size_bucket,
    COUNT(*)                                                AS fund_count,
    ROUND(SUM(total_offering_amount) / 1e9, 2)             AS total_capital_bn,
    ROUND(COUNT(*) * 100.0 /
          (SELECT COUNT(*) FROM form_d WHERE quality_tier = 'Clean'), 2)
                                                            AS pct_of_funds,
    ROUND(SUM(total_offering_amount) * 100.0 /
          (SELECT SUM(total_offering_amount)
           FROM form_d WHERE quality_tier = 'Clean'), 2)    AS pct_of_capital
FROM form_d
WHERE quality_tier = 'Clean'
GROUP BY fund_size_bucket
ORDER BY total_capital_bn DESC;


-- ── 5. INVESTOR CONCENTRATION ─────────────────────────────────────────────────
SELECT
    investor_bucket,
    COUNT(*)                                                AS fund_count,
    ROUND(SUM(total_offering_amount) / 1e9, 2)             AS capital_raised_bn,
    ROUND(AVG(total_offering_amount) / 1e6, 2)             AS avg_fund_size_mn,
    ROUND(SUM(total_offering_amount) * 100.0 /
          (SELECT SUM(total_offering_amount)
           FROM form_d WHERE quality_tier = 'Clean'), 2)    AS pct_of_total_capital
FROM form_d
WHERE quality_tier = 'Clean'
GROUP BY investor_bucket
ORDER BY capital_raised_bn DESC;


-- ── 6. YEAR-OVER-YEAR TRENDS BY ASSET CLASS ──────────────────────────────────
SELECT
    filing_year,
    asset_class_group,
    COUNT(*)                                                AS fund_count,
    ROUND(SUM(total_offering_amount) / 1e9, 2)             AS capital_raised_bn,
    ROUND(AVG(total_offering_amount) / 1e6, 2)             AS avg_fund_size_mn
FROM form_d
WHERE quality_tier = 'Clean'
  AND filing_year BETWEEN 2022 AND 2024
GROUP BY filing_year, asset_class_group
ORDER BY filing_year ASC, capital_raised_bn DESC;


-- ── 7. CLOSE RATE BY ASSET CLASS ─────────────────────────────────────────────
SELECT
    asset_class_group,
    fund_type,
    COUNT(*)                                                AS fund_count,
    ROUND(AVG(pct_capital_raised) * 100, 2)                AS avg_close_rate_pct,
    ROUND(MIN(pct_capital_raised) * 100, 2)                AS min_close_rate_pct,
    ROUND(MAX(pct_capital_raised) * 100, 2)                AS max_close_rate_pct
FROM form_d
WHERE quality_tier = 'Clean'
  AND pct_capital_raised IS NOT NULL
GROUP BY asset_class_group, fund_type
ORDER BY avg_close_rate_pct DESC;


-- ── 8. DUPLICATE FILING DETECTION ────────────────────────────────────────────
-- Same fund filing multiple Form Ds in the same quarter (D/A amendments)
SELECT
    issuer_name,
    filing_quarter,
    federal_exemption,
    COUNT(*)                                                AS filing_count,
    ROUND(SUM(total_offering_amount) / 1e6, 2)             AS total_offered_mn
FROM form_d
GROUP BY issuer_name, filing_quarter, federal_exemption
HAVING COUNT(*) > 1
ORDER BY filing_count DESC
LIMIT 20;


-- ── 9. DATA QUALITY TIER DISTRIBUTION ────────────────────────────────────────
SELECT
    quality_tier,
    COUNT(*)                                                AS record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM form_d), 2)
                                                            AS pct_of_total
FROM form_d
GROUP BY quality_tier
ORDER BY record_count DESC;


-- ── 10. TOP FUNDRAISES BY OFFERING SIZE ──────────────────────────────────────
SELECT
    issuer_name,
    fund_type,
    asset_class_group,
    federal_exemption,
    filing_date,
    ROUND(total_offering_amount / 1e6, 2)                  AS offering_mn,
    ROUND(amount_sold / 1e6, 2)                            AS raised_mn,
    ROUND(pct_capital_raised * 100, 1)                     AS close_rate_pct,
    num_investors
FROM form_d
WHERE quality_tier = 'Clean'
ORDER BY total_offering_amount DESC
LIMIT 25;


-- ── 11. VINTAGE YEAR COHORT ANALYSIS ─────────────────────────────────────────
-- Fundraising performance by vintage year — mirrors PE benchmarking
SELECT
    filing_year                                             AS vintage_year,
    fund_type,
    COUNT(*)                                                AS fund_count,
    ROUND(SUM(total_offering_amount) / 1e9, 2)             AS total_target_bn,
    ROUND(SUM(amount_sold) / 1e9, 2)                       AS total_closed_bn,
    ROUND(
        SUM(amount_sold) / NULLIF(SUM(total_offering_amount), 0) * 100, 2
    )                                                       AS overall_close_rate_pct
FROM form_d
WHERE quality_tier = 'Clean'
GROUP BY filing_year, fund_type
ORDER BY filing_year ASC, total_closed_bn DESC;


-- ── 12. FIELD-LEVEL COMPLETENESS CHECK ───────────────────────────────────────
-- Database health check — mirrors data quality audits run by data ops teams
SELECT 'total_offering_amount'                              AS field_name,
    COUNT(*)                                                AS total_records,
    SUM(CASE WHEN total_offering_amount IS NULL THEN 1 ELSE 0 END)
                                                            AS missing_count,
    ROUND(SUM(CASE WHEN total_offering_amount IS NOT NULL THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)                           AS completeness_pct
FROM form_d
UNION ALL
SELECT 'federal_exemption', COUNT(*),
    SUM(CASE WHEN federal_exemption IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN federal_exemption IS NOT NULL THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)
FROM form_d
UNION ALL
SELECT 'num_investors', COUNT(*),
    SUM(CASE WHEN num_investors IS NULL OR num_investors = 0 THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN num_investors > 0 THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)
FROM form_d
UNION ALL
SELECT 'pct_capital_raised', COUNT(*),
    SUM(CASE WHEN pct_capital_raised IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN pct_capital_raised IS NOT NULL THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)
FROM form_d
ORDER BY completeness_pct ASC;
