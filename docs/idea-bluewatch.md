# App Idea: BlueWatch — Satellite Chlorophyll Anomaly Alerts for Coastal HAB Early Warning
*Logged: April 2026 | Status: Rough Idea*

---

## The Pitch

BlueWatch is a web and mobile app that turns free Copernicus Marine Service satellite chlorophyll-a data into configurable push alerts for coastal managers, shellfish farms, salmon operators, and beach safety authorities — telling them when phytoplankton concentrations in their defined sea area are anomalously elevated relative to seasonal norms, before a harmful algal bloom becomes visible or reportable. The 2025 South Australian Karenia bloom — which killed over 500 species across 4,400 km² of coastline, cost A$102.5 million in government support, and shuttered oyster farms and fisheries for months — was tracked by a volunteer-built Streamlit script and iNaturalist observations. No purpose-built alert infrastructure existed. BlueWatch builds what should have been there already.

---

## The Opportunity

Harmful algal blooms are increasing in frequency, duration, and geographic range as sea surface temperatures rise and coastal eutrophication accelerates. The economic costs are concrete: Florida's 2018 Karenia brevis red tide cost an estimated $2.7 billion. HABs cost US commercial fisheries and aquaculture $1.5–5.6 billion annually. The 2025 South Australian event alone triggered A$102.5 million in emergency government spending and created a new National Office for Algal Bloom Research.

The core data problem is solved. Copernicus Marine Service delivers daily L3 chlorophyll-a products from Sentinel-3 OLCI at 300m resolution, free globally, updated within 3 hours of acquisition. The anomaly calculation — current Chl-a versus the climatological mean for that pixel and calendar week — is standard oceanographic practice. The scientific methodology is well-established in the peer-reviewed literature.

What does not exist is a non-technical interface that wraps this data into configurable threshold alerts for the operators who need it most. HABreports.org (SAMS) is the closest existing tool — a working early warning system for Scottish waters combining satellite and modelling data. It is grant-funded, covers only Scottish waters, requires expert interpretation by SAMS scientists, and is not a scalable product. It is a bespoke service for one country's industry.

The global aquaculture market is valued at $310 billion (2024), growing at ~5% CAGR. Scotland alone generates £468 million GVA from aquaculture. Norway, Chile, Australia, New Zealand, Canada, Ireland, and Japan collectively represent a large, motivated commercial operator base with real economic exposure to HAB events. None of them have access to a configurable, satellite-based threshold alert system they can point at their own farm sites.

**The policy window is unusually open.** The 2025 SA bloom generated emergency government spending and a new national research office. HABreports.org's Scottish model — government and industry co-funded at national scale — demonstrates institutional willingness to pay. The absence of a commercial equivalent is not evidence of lack of demand; it is evidence of no one having built it.

---

## Target User

**Primary (B2B — paying tier):** Commercial aquaculture operators with coastal marine sites. Shellfish farms (mussels, oysters, clams) are the highest-priority segment — bivalves bioaccumulate algal toxins directly, leading to regulatory harvest closures with immediate revenue loss. Salmon cage operators face fish-killing species (Karenia mikimotoi, Chrysochromulina) rather than toxin issues, but the economic consequences are equivalent. A single farm manager responsible for a site worth £1–10 million in stock is a self-evidently viable paying user.

Target geographies: Scotland, Norway, Ireland, France (Brittany/Normandy), Chile, Australia (SA, Tasmania, WA), New Zealand, Canada (BC, New Brunswick).

**Secondary (free tier or grant-funded institutional access):** Coastal councils and beach management authorities. Marine protected area managers. Coastal environmental NGOs. University research groups requiring near-real-time Chl-a anomaly data for specific study sites.

**Tertiary (public):** Surfers, dive operators, wild swimming communities — anyone who wants to understand current phytoplankton conditions without navigating CMEMS's technical interfaces.

**Not the target user:** Inland aquaculture (freshwater), users expecting species-level identification from satellite data. Chl-a is a proxy for total phytoplankton biomass; it cannot distinguish toxic from non-toxic species. This must be stated clearly.

---

## Core Feature Set (v1)

**Zone Definition**
- User draws a polygon on a map (or uploads GeoJSON) defining their monitoring area
- Minimum polygon size enforced to avoid sub-pixel queries on the 300m CMEMS product
- Multiple zones per account (paid tier)

**Anomaly Alert Engine**
- Daily ingestion of CMEMS L3 NRT (near-real-time) chlorophyll-a product for global coastal zones
- Per-pixel anomaly calculated against a pre-computed climatological mean (CMEMS MY product, 2016–2024) for the same calendar week
- Zone-averaged anomaly score computed across the user's polygon
- Configurable threshold: alert when zone average exceeds N× seasonal mean (default: 3×; adjustable 1.5×–10×)
- Push notification (mobile) + email digest when threshold is crossed

**Contextual Alert Card**
- When threshold exceeded: current Chl-a value, seasonal mean, anomaly ratio, previous 30-day trend chart for the zone
- SST overlay (from CMEMS SST NRT product, free): elevated SST + Chl-a anomaly = elevated risk framing
- Wind speed and direction (ERA5 reanalysis or open-meteo API): onshore wind increases coastal risk
- Plain-language risk framing combining all three signals — e.g. "Phytoplankton in your zone is 4.2× the July seasonal average. Combined with elevated SST (+1.8°C above mean) and onshore winds, conditions are consistent with bloom initiation. Recommend visual inspection."

**Cloud Gap Handling (critical UX requirement)**
- Sentinel-3 OLCI is optical: gaps occur under persistent cloud cover (common in Scotland, Norway, Chile)
- Explicit "No satellite data — cloud cover" status for gap periods; no silent failure
- L4 gap-filled CMEMS product available as optional fallback, clearly flagged as interpolated
- Data availability calendar per zone ("Last 14 days: 9 clear, 5 cloud-gap")

**Historical Archive**
- 30-day rolling chart for any zone (free tier)
- Full historical access to 2016 Sentinel-3 launch (paid tier)
- Year-on-year comparison: "This July is the highest Chl-a in your zone's 9-year record"

**Species Context Layer (v1 stretch)**
- Static reference layer showing known HAB species by region and season, sourced from IOC-UNESCO HAB records and WHOI HAB database
- Not real-time species identification — clearly communicated. Satellite Chl-a cannot distinguish species.

---

## Technical Feasibility Summary

This matters enough to include in the rough document because the engine analysis (April 2026) directly addresses whether the product is buildable.

**BlueWatch is an anomaly detector, not a forecast system.** This is the defining technical constraint. The moment framing shifts toward "BlueWatch predicts where the bloom will be in 5 days," the engineering requirement balloons by 10–100× and scientific reliability declines substantially. Trajectory forecasting (as HABreports.org does with WeStCOMS) requires regional hydrodynamic modelling expertise, dedicated HPC, and 12–24 months of build time. ML bloom prediction requires longitudinal in-situ training data that does not exist globally. Neither is in scope for v1, v2, or plausibly v3.

**The in-scope pipeline is tractable.** The Chl-a anomaly engine (2–3 months), SST context layer (2–4 weeks), wind context (days), cloud gap handling with L4 fallback (1–2 months), alert dispatch (1–2 weeks), and turbid coastal pixel masking (3–4 weeks) can be built by two people — one with oceanographic domain expertise, one software engineer — in approximately 4–6 months. Every element uses existing open-source tooling (xarray, the CMEMS Python toolbox, regionmask) and free satellite data.

**The hardest problems are signal quality, not engineering.** Case 2 coastal waters with high turbidity (CDOM, suspended sediment) produce unreliable Chl-a retrievals and high false positive rates. Turbid estuaries and sheltered inner bays — which are disproportionately where aquaculture happens — are exactly where the standard CMEMS OC5 algorithm performs worst. The initial market should be segmented toward cleaner, open coastal waters. Per-region calibration of alert thresholds is required before each new geography is opened. This is where marine scientist founders have a structural advantage over a software-first team.

---

## Monetisation

**Freemium SaaS:**

| Tier | Price | Features |
|---|---|---|
| Free | £0 | 1 monitoring zone (max 50 km²), weekly digest only, 30-day history, public Chl-a anomaly map |
| Professional | ~£25/month or £240/year per site | Real-time daily alerts, unlimited zone size, SST + wind context, 9-year archive, CSV/GeoJSON export, API access |
| Enterprise | ~£150–500/month (negotiated) | Unlimited sites, white-label option, custom alert logic, integration with farm management dashboards |

**Revenue model:**
- 100 Professional sites × £240/year = £24,000 ARR
- 500 Professional sites × £240/year = £120,000 ARR (viable small product)
- 2,000 Professional sites × £240/year = £480,000 ARR (strong indie, covers infrastructure + small team)
- 50 Enterprise accounts × £3,000/year average = £150,000 ARR incremental

Scotland has ~250 active aquaculture sites. Norway ~1,000. Ireland ~500. Australia ~2,000. The addressable paying market across major aquaculture nations in Europe, Australasia, and the Americas is conservatively 15,000–25,000 individual farm sites. 2% conversion at £240/year is meaningful.

**Grant revenue (parallel, not primary):**
Copernicus User Uptake programme funds downstream applications of Copernicus data — BlueWatch is a textbook candidate. EMFAF (European Maritime and Fisheries Fund) funds coastal environmental monitoring tools. BBSRC, NERC, and Innovate UK fund marine technology with commercial application. These fund infrastructure development, not ongoing operations.

**B2B distribution angle:** RYA training centres and aquaculture industry associations (e.g. SSPO in Scotland, Seafish) already coordinate with member farms on monitoring protocols. A recommended tool per association is a scalable distribution channel that doesn't require farm-by-farm cold outreach.

---

## Competitive Landscape

| Tool / Service | Coverage | Type | Core Weakness |
|---|---|---|---|
| HABreports.org (SAMS) | Scotland only | Government/industry funded | Not scalable; expert-dependent; Scotland-specific; not a product |
| CMEMS Ocean Colour Portal | Global | Free data portal | Technical interface; no alerts; NetCDF download only |
| NOAA HAB Forecast Bulletins | US coastal | Weekly PDF bulletins (free) | Weekly cadence; US only; not configurable |
| CalHABMAP | California | State government map | State-specific; map only; no push alerts |
| CyanoTRACKER | Freshwater/coastal | Citizen science report app | Reactive (users report visible blooms); not prospective |
| Bloomin' Algae (EA, UK) | UK freshwater | Citizen science report app | Freshwater focus; reactive; no satellite layer |
| Copernicus Marine App | Global | Official data viewer | No alerts; no anomaly calculation; raw data only |
| Spire / exactEarth | Global | Enterprise maritime intelligence | Not HAB-focused; enterprise pricing; wrong use case |
| SARDI monitoring website | South Australia | Government portal | Created ad hoc during 2025 crisis; no ongoing operational model |
| iNaturalist HAB observations | Global | Citizen science | Reactive community observation; not satellite-based |

**The honest competitive position:** HABreports.org is the most direct predecessor and demonstrates that the demand is real, the science works, and the aquaculture sector will engage. Its weakness is that it is a manually operated, government-funded regional service — not a scalable, self-serve global product. BlueWatch is the product version of what HABreports.org proved is needed.

---

## Key Differentiators

1. **Configurable thresholds for user-defined zones** — No existing tool lets a farm manager draw their actual site on a map and receive an alert when Chl-a exceeds their chosen threshold. This is the core v1 feature and it does not exist anywhere in the market today.

2. **Prospective, not reactive** — Every existing citizen science tool requires someone to see a bloom before reporting it. BlueWatch alerts 24–72 hours before surface bloom formation becomes visually apparent, using satellite precursor signals. That lead time is the economic value: it allows a shellfish operator to delay harvest rather than discover toxin contamination in product already bagged.

3. **Cloud gap transparency as a first-class UX requirement** — The 2025 SA bloom was poorly tracked partly because satellite data has cloud-cover gaps. An alert system that silently fails during gaps is dangerous. Treating gap communication as a primary UX requirement, not an afterthought, is a genuine differentiator.

4. **Marine science domain expertise baked into the alert text** — The distinction between a nuisance false positive (spring diatom bloom) and a genuine HAB precursor signal involves SST, thermal stratification, wind-driven upwelling patterns, and regional species ecology. This is not in the satellite data — it has to be built into the alert framing. A generic development team cannot replicate this without the background.

5. **Global from day one** — CMEMS data is global. HABreports.org solved the problem for one country's industry; there is no structural reason this can't be a global product from day one, with regional expansion of contextual layers over time.

---

## Risks & Open Questions

**False positive rate in turbid coastal waters.** High Chl-a anomaly is not synonymous with HAB risk. Spring phytoplankton blooms are typically benign. In turbid, shallow, or CDOM-rich coastal embayments — which are disproportionately where aquaculture operates — the standard CMEMS OC5 algorithm confuses CDOM and sediment with chlorophyll, producing false positive rates that can approach 80%+ without additional filtering. The mitigation is initial market segmentation toward cleaner, open coastal waters and systematic per-region threshold calibration before each new geography. This is not trivial — it requires oceanographic judgement per region and is the core scientific risk in the product.

**Cloud cover as a structural data gap.** Persistent cloud cover in UK, Norwegian, and Chilean aquaculture waters produces multi-day and multi-week CMEMS data gaps. The L4 interpolated product partially addresses this but introduces spatial uncertainty. An operator who treats silence as reassurance will be worse off than one with no tool at all. This must be communicated relentlessly from day one.

**Cry-wolf problem.** If users receive frequent alerts that don't correspond to real HAB events, they disengage — and that's worse than no system. Multi-signal thresholds mitigate this, as does onboarding that sets accurate expectations. The alert text quality is not decorative; it is load-bearing.

**Sentinel-3 product continuity.** The current system relies on Sentinel-3A and 3B OLCI. ESA plans Sentinel-3C but there are operational continuity gaps. MODIS-Aqua and VIIRS (NASA) provide fallback Chl-a products, also free. A multi-sensor approach reduces this risk but increases processing complexity.

**HABreports.org incumbency expansion risk.** SAMS could, with additional grant funding, expand HABreports.org to cover more geographies. The mitigation is speed of international deployment and the B2B freemium model — a grant-funded institutional service cannot iterate on pricing and UX the way a product can.

**Regulatory complexity.** In most jurisdictions, shellfish harvest closure decisions are made by government food safety agencies (FSA in the UK, FDA/NOAA in the US) based on toxin testing in flesh — not satellite Chl-a anomalies. BlueWatch is a precautionary signal tool, not a regulatory trigger. This must be explicit in all user communications. Get legal advice on disclaimer language before any beta launch.

**Market size ceiling.** The paying market is commercially serious but defined. If the global paying aquaculture operator base is ~15,000–25,000 sites and conversion is 2–5%, this is a strong indie product or a strategic acquisition target (by an aquaculture intelligence platform, an agri-data company, or a government environmental agency). It is not a VC-scale outcome on its own.

**Scope creep toward forecasting is the existential threat.** The moment stakeholders start asking "but can it predict where the bloom will go?" the engineering requirement and scientific reliability both shift dramatically. Trajectory forecasting requires regional hydrodynamic modelling expertise that is orthogonal to remote sensing science, and the regional commitment (you build a Scottish model, not a global model) contradicts the global thesis. The right long-term path for forecasting is partnership with existing modelling centres (SAMS, IFREMER, CSIRO), not building in-house.

---

## Revenue Benchmark

HABreports.org's funding model — Scottish Government plus Seafood Shetland industry contributions across multiple EU and UK grants since the ASIMUTH project — demonstrates that the aquaculture sector will fund early warning systems when they exist and work. The 2025 SA bloom created a new national research office and A$102.5 million in emergency spending. At modest scale, 500 professional site subscriptions at £240/year = £120,000 ARR with minimal infrastructure cost and a market that has demonstrated strong economic motivation to pay for the thing being built.

---

## Next Steps (When Ready to Explore)

- [ ] Contact SAMS to understand HABreports.org's architecture, data pipeline, and whether partnership or licensing is viable vs. building independently
- [ ] Audit CMEMS L3 NRT ocean colour API: query latency, polygon support, rate limits, authentication for production use
- [ ] Build proof-of-concept anomaly alert for one specific coastal polygon (e.g. Shetland or a SA/Tasmanian salmon zone) using historical CMEMS data — validate that the anomaly signal precedes known bloom events by 24–72 hours in the archive
- [ ] Identify 5–10 aquaculture operators for initial discovery interviews: what monitoring do they currently do? What would a 48-hour advance alert be worth? Would they pay monthly?
- [ ] Confirm the CMEMS L4 gap-filled product's suitability for alert function in cloud-prone regions (Norway, Scotland, BC)
- [ ] Assess Copernicus User Uptake funding programme timeline for applications
- [ ] Define explicit disclaimer/liability language around alerts vs. regulatory harvest decisions — get early legal advice before any beta launch
- [ ] Research EMFAF and Innovate UK for open calls compatible with marine environmental monitoring tools
- [ ] Map the initial market to open coastal, low-turbidity aquaculture sites in Europe as the v1 target — defer turbid-water embayments until per-region algorithm work is done

---

*Filed under: /rough ideas | Do not share externally*
