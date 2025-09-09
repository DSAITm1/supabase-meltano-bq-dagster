# Customer Analytics Report

**Project**: Marketing Analytics Dashboard - Olist E-commerce  
**Report Period**: All-time Customer Analysis  
**Generated**: September 7, 2025  
**Data Source**: `project-olist-470307.dbt_olist_analytics`

---

## 📊 Executive Summary

Our customer analytics reveal a **growing customer base of 95,419 active customers** across Brazil, generating **$13.59M in total revenue**. The analysis shows significant opportunities for customer retention and value optimization, with 97% of customers being one-time or occasional buyers.

### Key Performance Indicators
- **Total Active Customers**: 95,419
- **Geographic Coverage**: 27 states, 4,109 cities
- **Average Customer Value**: $142.44
- **Average Order Value**: $138.23
- **Customer Satisfaction**: 4.06/5 ⭐
- **Predicted Annual CLV**: $108,971

---

## 🎯 Customer Segmentation Analysis

Segmentation uses an RFM (Recency / Frequency / Monetary) + behavior framework. Below is a consolidated view combining definitions, criteria, size, value and recommended action for each segment (ordered by average monetary value / strategic leverage).

| Segment | Definition (Plain) | Core Criteria (Simplified) | Customers | % Base | Avg Spent | Primary Objective | Recommended Action |
|---------|--------------------|----------------------------|-----------|--------|-----------|-------------------|-------------------|
| 🏆 Champions | Highest value & most recently active multi‑purchase customers; brand advocates | High recency; high frequency (≥4); monetary > $500 | 12 | 0.01% | $826.00 | Retain & leverage advocacy | VIP perks, early access, referral incentives |
| 💎 Loyal Customers | Consistent repeat purchasers with strong spend | Good recency; high frequency (≥3); monetary > $400 | 113 | 0.12% | $630.23 | Deepen loyalty & referrals | Tiered rewards, exclusive bundles |
| 🌱 Potential Loyalists | Emerging repeat buyers showing upward value trend | Recent purchase; medium frequency (2–3); rising spend | 1,651 | 1.73% | $347.85 | Accelerate to loyal | Personalized cross-sell, timed re‑engagement |
| 🆕 New (High Value) | Recent first/second purchase with high initial basket | High recency; frequency 1–2; first order > $200 | 38,580 | 40.42% | $258.10 | Secure 2nd / 3rd purchase | Onboarding sequence, 2nd purchase incentive |
| 😴 Hibernating | Previously active, now lapsed beyond recency threshold | Low recency; historical frequency ≥2; declining engagement | 1,137 | 1.19% | $94.86 | Reactivate or churn confirm | Win‑back offer, reminder of value, feedback survey |
| 🆕 New (Low Value) | Recent single low-spend purchase; low commitment | High recency; frequency 1; order < $100 | 53,926 | 56.52% | $53.24 | Nurture & increase AOV | Educational content, bundles, progressive offers |

Notes:
1. % Base is relative to 95,419 active customers.
2. Movement priority: New (Low Value) → New (High Value) → Potential Loyalist → Loyal → Champion.
3. Reactivation ROI threshold: If Hibernating reactivation cost >20% of projected next 12‑month value, deprioritize.

---

## 🗺️ Geographic Distribution

### Top Performing States

| State | Total Unique Customers | Total Orders | Total Revenue | Avg Customer Value | Avg Order Value | Market Share | Top 3 Product Categories |
|-------|----------------------|--------------|---------------|-------------------|-----------------|--------------|--------------------------|
| **São Paulo (SP)** | 39,963 | 41,357 | $5,201,471 | $130.14 | $126.06 | 41.9% | bed_bath_table, health_beauty, sports_leisure |
| **Rio de Janeiro (RJ)** | 12,300 | 12,763 | $1,823,989 | $148.37 | $143.26 | 12.9% | bed_bath_table, health_beauty, sports_leisure |
| **Minas Gerais (MG)** | 11,176 | 11,549 | $1,585,699 | $141.89 | $137.82 | 11.7% | bed_bath_table, health_beauty, sports_leisure |
| **Rio Grande do Sul (RS)** | 5,248 | 5,432 | $750,332 | $143.00 | $138.29 | 5.5% | bed_bath_table, health_beauty, sports_leisure |
| **Paraná (PR)** | 4,840 | 5,002 | $683,547 | $141.25 | $137.32 | 5.1% | bed_bath_table, health_beauty, sports_leisure |
| **Santa Catarina (SC)** | 3,509 | 3,611 | $520,278 | $148.27 | $143.78 | 3.7% | bed_bath_table, health_beauty, sports_leisure |
| **Bahia (BA)** | 3,257 | 3,360 | $511,613 | $157.11 | $152.85 | 3.4% | bed_bath_table, health_beauty, sports_leisure |
| **Distrito Federal (DF)** | 2,062 | 2,129 | $302,876 | $146.93 | $143.63 | 2.2% | bed_bath_table, health_beauty, sports_leisure |

### Geographic Strategy Insights

#### 🎯 Market Concentration
- **Top 3 states** account for **66.5%** of total customer base
- **São Paulo dominance**: 41.9% of customers, 38.3% of revenue
- **Top 8 states** represent **85.7%** of total customer base
- **Geographic diversification opportunity** in remaining 19 states

#### 💰 Revenue per Customer Analysis
- **Bahia (BA)** shows highest customer value at $157.11
- **Santa Catarina (SC)** and **Rio de Janeiro (RJ)** also perform above average ($148+)
- **São Paulo** slightly below average at $130.14 despite volume
- **Opportunity**: Increase AOV in high-volume markets

#### 📦 Product Category Insights
- **Consistent top categories** across all major states: bed_bath_table, health_beauty, sports_leisure
- **Universal appeal** of home essentials and personal care products
- **Opportunity**: State-specific category promotions and regional product variations

---

## 🛒 Purchase Behavior Analysis

### Customer Purchase Frequency

| Behavior Pattern | Customers | % of Base | Avg Spent | Retention Risk |
|------------------|-----------|-----------|-----------|----------------|
| **One-time Buyers** | 92,506 | 96.95% | $138.67 | Very High |
| **Occasional Buyers** | 2,865 | 3.00% | $255.46 | High |
| **Regular Buyers** | 43 | 0.05% | $636.81 | Medium |
| **Frequent Buyers** | 5 | 0.01% | $802.10 | Low |

### 🚨 Critical Findings

#### Customer Retention Crisis
- **96.95% are one-time buyers** - massive retention opportunity
- Only **3.05%** make repeat purchases
- **Urgent need** for retention strategy implementation

#### Value Correlation
- Clear correlation between purchase frequency and customer value
- **Frequent buyers** spend 5.8x more than one-time buyers
- **Regular buyers** show 4.6x higher value

---

## 📈 Revenue Opportunities

### 1. Customer Retention Strategy
**Impact**: Convert 10% of one-time buyers to occasional buyers
- **Potential**: 9,251 customers × $116.79 additional spend = **$1.08M revenue increase**
- **Actions**: Welcome series, product recommendations, loyalty program

### 2. Geographic Expansion
**Impact**: Increase market penetration in underperforming states
- **Potential**: Focus on states with <1,000 customers
- **Actions**: Regional marketing campaigns, local partnerships

### 3. Segment Elevation
**Impact**: Move customers up the value ladder
- **New Customer (Low Value) → High Value**: 5% conversion = **$1.11M revenue**
- **Potential Loyalists → Loyal**: 25% conversion = **$482K revenue**

### 4. Average Order Value Optimization
**Impact**: Increase AOV across all segments
- **5% AOV increase** across customer base = **$679K additional revenue**
- **Focus**: Cross-selling, bundle offers, minimum order incentives

---

## 🎨 Marketing Recommendations

### Immediate Actions (Next 30 Days)

#### 🔄 Retention Campaign Launch
1. **Welcome Series**: 3-email sequence for new customers
2. **Second Purchase Incentive**: 15% discount within 60 days
3. **Product Recommendations**: Based on purchase history

#### 📧 Segment-Specific Campaigns
1. **Champions/Loyal**: VIP program launch
2. **Potential Loyalists**: Personalization increase
3. **New Customers**: Educational content series
4. **Hibernating**: Win-back offer (25% discount)

### Medium-term Strategy (Next 90 Days)

#### 🏗️ Program Development
1. **Loyalty Program**: Points-based system with tier benefits
2. **Referral Program**: Leverage satisfied customers
3. **Geographic Expansion**: Marketing in underperforming states

#### 📊 Data & Analytics
1. **Customer Journey Mapping**: Identify friction points
2. **Cohort Analysis**: Track retention improvements
3. **Predictive Modeling**: Identify churn risk early

### Long-term Vision (Next 12 Months)

#### 🚀 Customer Experience Enhancement
1. **Personalization Engine**: AI-driven recommendations
2. **Customer Success Program**: Proactive support
3. **Community Building**: Customer forums and events

---

## 📋 Success Metrics & KPIs

### Primary Metrics
- **Customer Retention Rate**: Target 25% (from current ~3%)
- **Average Customer Lifetime Value**: Increase by 30%
- **Repeat Purchase Rate**: Target 15% within 6 months
- **Customer Satisfaction**: Maintain above 4.0

### Secondary Metrics
- **Geographic Distribution**: Reduce concentration risk
- **Segment Migration**: Move customers up value tiers
- **Revenue per Customer**: Increase by 20%
- **New Customer Conversion**: Improve onboarding effectiveness

---

## 🔬 Data Quality & Methodology

### Data Sources
- **Primary**: `customer_analytics_obt` - Customer behavior and segmentation
- **Supporting**: `revenue_analytics_obt` - Purchase and transaction data
- **Geographic**: `geographic_analytics_obt` - Market performance data

### Analysis Period
- **Coverage**: All-time customer data
- **Last Updated**: September 2025
- **Data Quality**: 95,419 active customers with complete profiles

### Limitations
- Seasonal trends not captured in all-time analysis
- Customer acquisition costs not included in CLV calculations
- External market factors not considered in projections

---

## 🎯 Conclusion

Our customer analytics reveal a **massive opportunity for retention improvement**. While we have successfully acquired 95,419 customers across Brazil, the challenge lies in converting one-time buyers into loyal, repeat customers.

### Key Takeaways:
1. **Retention is the #1 priority** - 97% one-time buyer rate needs immediate attention
2. **Geographic concentration** presents both strength and risk
3. **Customer value increases dramatically** with purchase frequency
4. **Segment-specific strategies** will maximize conversion rates

### Expected Impact:
Implementation of recommended strategies could potentially:
- **Increase revenue by $3.2M+** annually
- **Improve customer retention by 22 percentage points**
- **Enhance customer lifetime value by 30%**
- **Reduce geographic concentration risk**

**Next Steps**: Prioritize retention campaign launch and begin segment-specific marketing initiatives to capture the identified revenue opportunities.

---

*Report prepared by Marketing Analytics Dashboard Team*  
*For questions or additional analysis, contact the data team*
