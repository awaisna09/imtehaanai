#!/usr/bin/env python3
"""
LLM Service - Handles Large Language Model operations
"""

import json
import logging
import os
import time
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Import metrics service for tracking (non-blocking, failure-safe)
try:
    from services.metrics import metrics_service
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    metrics_service = None

# ============================================================================
# PHASE 3: Prompt Template Caching
# ============================================================================
# Cache compiled prompt templates to avoid repeated string operations
_subject_prompt_cache = {}
_subject_base_prompt_cache = {}


class LLMService:
    """
    Handles LLM response generation and prompt management.
    """

    def __init__(self, llm, langchain_available: bool):
        """
        Initialize LLMService.

        Args:
            llm: LangChain ChatOpenAI instance (or None if unavailable)
            langchain_available: Whether LangChain is available
        """
        self.llm = llm
        self.langchain_available = langchain_available
        self.logger = logging.getLogger(__name__)
        # Initialize OpenAI client for direct API calls (e.g., summarization)
        try:
            import os
            from openai import OpenAI
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key:
                self.openai_client = OpenAI(api_key=api_key)
            else:
                self.openai_client = None
        except ImportError:
            self.openai_client = None

    def generate_reply(
        self,
        message: str,
        topic: str,
        learning_level: str,
        conversation_history: List[Dict],
        lesson_content: Optional[str] = None,
        concept_rows: Optional[List[Dict]] = None,
        explanation_style: str = "default",
        lesson_chunks: Optional[List[Dict]] = None,
        condensed_history: Optional[str] = None,
        student_profile: Optional[Dict] = None,
        subject_id: Optional[int] = None,
        subject_name: Optional[str] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> tuple:
        """
        Generate AI tutor reply. Uses LangChain if available, otherwise
        fallback.

        Args:
            message: Student's message/question
            topic: Topic/subject
            learning_level: Student's learning level
            conversation_history: List of conversation messages
            lesson_content: Optional lesson content
            concept_rows: Optional related concepts
            explanation_style: Explanation style preference
            lesson_chunks: Optional relevant lesson chunks
            condensed_history: Optional condensed history text
            student_profile: Optional student profile

        Returns:
            tuple: (response_text, token_usage_dict, reasoning_label)
            - response_text: The tutor's response
            - token_usage_dict: {
                "prompt_tokens": int,
                "completion_tokens": int,
                "total_tokens": int
              }
            - reasoning_label: 'good', 'neutral', or 'confused'
        """
        if self.langchain_available and self.llm:
            try:
                return self._generate_with_langchain(
                    message,
                    topic,
                    learning_level,
                    conversation_history,
                    lesson_content,
                    concept_rows,
                    explanation_style,
                    lesson_chunks,
                    condensed_history,
                    student_profile,
                    subject_id,
                    subject_name,
                    job_id=job_id,
                    trace_id=trace_id
                )
            except Exception:
                subject_name_actual = (
                    subject_name or
                    (self._get_subject_name_from_id(subject_id)
                     if subject_id else None)
                )
                fallback_response = self._generate_fallback_response(
                    message, topic, subject_name_actual
                )
                return (fallback_response, {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0
                }, "neutral")  # Default reasoning_label for fallback
        else:
            subject_name_actual = (
                subject_name or
                (self._get_subject_name_from_id(subject_id)
                 if subject_id else None)
            )
            fallback_response = self._generate_fallback_response(
                message, topic, subject_name_actual
            )
            return (fallback_response, {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0
            }, "neutral")  # Default reasoning_label for fallback

    def _get_subject_prompt(
        self, 
        subject_id: Optional[int], 
        subject_name: Optional[str] = None
    ) -> str:
        """
        PHASE 3: Get subject-specific prompt with caching.
        Caches compiled templates to avoid repeated string operations.
        """
        # Map subject_id to subject name
        subject_map = {
            101: "Business Studies",
            102: "Islamiyat",
            113: "Pak Studies Geography",
            114: "Pak Studies History",
            119: "Economics",
            103: "Mathematics",
            104: "Physics",
            105: "Chemistry"
        }
        
        if not subject_name:
            subject_name = subject_map.get(subject_id, "Business Studies")
        
        # PHASE 3: Check cache first
        cache_key = f"{subject_id}:{subject_name}"
        if cache_key in _subject_prompt_cache:
            return _subject_prompt_cache[cache_key]
        
        # Subject-specific topic lists (EXPANDED for comprehensive coverage)
        subject_topics = {
            "Business Studies": """
- Business organizations (sole traders, partnerships, limited companies, PLCs, franchises, cooperatives)
- Business finance and accounting (profit and loss, balance sheets, cash flow, financial statements, budgets)
- Marketing and market research (4Ps, product life cycle, market segmentation, advertising, branding)
- Human resources and management (recruitment, selection, training, motivation, leadership styles, organizational structure)
- Operations and production (production methods, quality control, inventory management, lean production, JIT)
- Business strategy and planning (SWOT analysis, business plans, strategic decisions, objectives, mission)
- Economics and business environment (demand, supply, market forces, economic factors, PEST analysis, competition)
- Business ethics and social responsibility (CSR, ethical practices, sustainability, stakeholder interests)
- International business (globalization, international trade, multinationals, tariffs, quotas, exchange rates)
- Entrepreneurship (startups, innovation, risk-taking, business opportunities, enterprise skills)
- Business Activity and enterprise (types of business activity, needs and wants, goods and services)
- Business objectives (profit, growth, survival, market share, social objectives, stakeholder objectives)
- Stakeholders (owners, employees, customers, suppliers, government, community, pressure groups)
- Business growth (organic growth, mergers, acquisitions, horizontal/vertical/conglomerate integration)
- External influences on business (competition, technology, legal factors, economic factors, social factors, environmental factors)""",
            
            "Economics": """
- The nature of the economic problem (scarcity, choice, opportunity cost, wants vs needs)
- The factors of production (land, labor, capital, entrepreneurship)
- Opportunity cost (the cost of the next best alternative foregone)
- Production possibility curve (PPC) diagrams (showing opportunity cost, economic growth, efficiency)
- Microeconomics and macroeconomics (individual vs aggregate economic behavior)
- The role of markets in allocating resources (price mechanism, market forces, resource allocation)
- Demand (law of demand, demand curve, factors affecting demand, shifts vs movements)
- Supply (law of supply, supply curve, factors affecting supply, shifts vs movements)
- Price determination (market equilibrium, equilibrium price and quantity, market clearing)
- Price changes (causes of price changes, effects on demand and supply, market adjustments)
- Price elasticity of demand (PED) (calculation, types, factors affecting elasticity, applications)
- Price elasticity of supply (PES) (calculation, types, factors affecting elasticity, time periods)
- Market structure (perfect competition, monopoly, oligopoly, monopolistic competition, market power)
- Market failure (externalities, public goods, merit goods, demerit goods, imperfect information, market power)
- Economic systems (market economic system, mixed economic system, command economy, free market)
- Mixed economic system (combination of market and government intervention, advantages, disadvantages)
- Money and banking (functions of money, banking system, central bank, commercial banks, monetary policy)
- Workers (labor market, wages, employment, unemployment, trade unions, labor supply and demand)
- Firms (business organizations, production, costs, revenue, objectives, profit maximization)
- Firms and production (production function, factors of production, productivity, efficiency)
- Firms costs, revenue and objectives (fixed costs, variable costs, total costs, average costs, marginal costs, revenue, profit maximization, other objectives)
- Households (consumption, saving, spending patterns, income distribution, consumer behavior)
- Trade unions (labor unions, collective bargaining, wage negotiations, labor market influence)
- The role of government (government intervention, regulation, public goods, market failure correction)
- The macroeconomic aims of government (economic growth, low unemployment, price stability, balance of payments)
- Monetary policy (interest rates, money supply, central bank policy, inflation control, economic stabilization)
- Fiscal policy (government spending, taxation, budget, deficit, surplus, economic stabilization)
- Supply-side policy (policies to increase productive capacity, improve efficiency, reduce costs)
- Economic growth (GDP growth, factors affecting growth, sustainable growth, development)
- Employment and unemployment (types of unemployment, causes, effects, measurement, policies)
- Inflation and deflation (causes, effects, measurement, control policies, price stability)
- Living Standards (GDP per capita, HDI, quality of life indicators, factors affecting living standards)
- Population (population growth, demographic changes, age structure, dependency ratio, economic impact)
- Developed and Less-developed Economies (characteristics, differences, development indicators, challenges)
- International Specialisation (comparative advantage, absolute advantage, gains from trade, specialization)
- Globalisation, Free Trade and Protection (globalization effects, free trade benefits, protectionism, tariffs, quotas, trade barriers)
- Foreign Exchange Rates (exchange rate determination, fixed vs floating rates, appreciation, depreciation, effects)
- Current Account of the Balance of Payments (exports, imports, trade balance, current account components, deficits, surpluses)
- Poverty (absolute poverty, relative poverty, causes, measurement, policies to reduce poverty)""",
            
            "Pak Studies History": """
- Pre-Partition Reformers and Leaders: Shah Waliullah (1703-1762), Haji Shariatullah, Syed Ahmed Shaheed Barelvi, Titu Mir
- Mughal Empire Decline: The Downfall of Mughal Empire, Geographical Factors, Political Factors, Social factors, Military Factors
- British Colonial Period: East India Company, British Expansion in India, The Black Hole Tragedy, Pitt's India Act (1784), The Vernacular Press Act (1878)
- Resistance Movements: Rani of Jhansi (Lakshmibai), Shivaji, Reasons for War of Independence, British Achievements in the War of Independence 1857
- Pakistan Movement Beginnings: Allama Iqbal's Presidential Address at Allahabad (1930), Chaudri Rehmat Ali (1897-1951) and the Pakistan Movement (1933)
- Key Resolutions and Acts: The Pakistan Resolution (1940), Partition of Bengal (1905), Why was Bengal partitioned?, Morley-Minto Reforms (1909), The Rowlatt Act (1919), Simla Deputation (1906)
- World War I Era: World War I (1914-1918), World War 1 and the Institution of Caliphate, Importance of Turkey and Caliphate to the Muslims
- Muslim League Formation: Creation of All India Muslim League (1906), Lucknow Pact (1916), Montague Chelmsford Reforms (1919), Hindus' Response, Reaction of Muslims
- Khilafat Movement: Causes for Failure of Khilafat Movement, Jallianwala Bagh Incident / Amritsar Massacre (1919)
- Constitutional Reforms: Simon Commission (1927), Nehru Report (1928), Jinnah's 14 Points (1929), Round Table Conferences (1930-32), Government of India Act (1935)
- Elections and Political Developments: Elections of (1937), Anti-Muslim Attitudes, Building A Nation
- World War II and Independence: World War II (1939), The Quit India Movement (1942), Cripps Mission (1942), Simla Conference (1945), 3rd June Plan (1947)
- Partition Process: The Cabinet Mission Plan (1946), The Direct Action Day (1946), The Gandhi-Jinnah Talks (1944), Radcliffe Award / Boundary Commission (1947), Problems of Partition and Nascent Pakistan State
- Quaid-e-Azam: Political Achievements of Quaid-E-Azam (1906-47), Earlier Life and Biography
- Post-Independence Leaders: Liaquat Ali Khan (1947-1951), Building A Government, Establishing National Security, Liaquat-Nehru Pact (1950), Public and Representative Officers Disqualification Act (PRODA), Objectives Resolution (1949)
- Constitutional Development: Basic Principles Committee, Constitutional Crisis (1954-55), One Unit Scheme (1955)
- Prime Ministers: Khawaja Nazimuddin (1948-1953), Reasons for The Creation of Bangladesh
- Ayub Khan Era: The Decade of Progress (1958-69), Fall of Ayub Khan from Power, Agricultural Reforms, Constitutional Reforms, Land Reforms
- Zulfiqar Ali Bhutto Era: Zulfiqar Ali Bhutto (1971-1977), Industrial Reforms, Simla Agreement (1972), General Elections
- Zia-ul-Haq Era: Zia-ul-Haq (1977-1988), Islamisation, Afghan Miracle, 1985 Elections
- Benazir Bhutto Eras: Benazir Bhutto's 1st Term (1988-90), Why did Benazir fall from power in her 1st tenure?, Benazir Bhutto's 2nd Term (1993-96)
- Nawaz Sharif Eras: Nawaz Sharif's 1st Term (1990-93), Why was Nawaz Sharif's first government dismissed?, Nawaz Sharif's 2nd Term (1996-98), Why was Martial law imposed on 12th October 1999?
- Foreign Relations: Pakistan's Relations with India, Pakistan's Relations with Bangladesh, Pakistan's Relations with Afghanistan, Pakistan's Relations with Iran, Pakistan's Relations with China, Pakistan's Relations with USSR, Pakistan's Relations with USA
- Domestic and Economic Policies: Domestic Policy, Economic Policy, Political Policy
- Language Development: How has the Pakistan government promoted the development of Urdu since 1947?, How has the Pakistan government promoted the development of Punjabi?, How has the Pakistan government promoted the development of Sindhi since 1947?
- Main Events and Services: Main Events, Services""",
            
            "Pak Studies Geography": """
- Administrative Areas and Major Named Cities (Islamabad, Karachi, Lahore, major cities and administrative divisions)
- Major Rivers of Pakistan (Indus, Jhelum, Chenab, Ravi, Sutlej, Beas, major river systems)
- Mountain Passes and the Regions They Connect (Karakoram Pass, Khunjerab Pass, Bolan Pass, Khyber Pass, mountain pass geography)
- Northern and North-Western Mountains: Karakoram, Himalayas (Siwaliks, Lesser Himalayas, Central Himalayas), and Hindu Kush ranges
- Western Mountains: Safed Koh, Waziristan Hills, Sulaiman Range, and Kirthar Range
- Plateaus: Balochistan Plateau (Basins and Hamuns) and Potwar Plateau (Badland Topography)
- Indus Plain: Upper Indus Plain, Lower Indus Plain, and Deltaic Plains, Including Doabs and Bars
- Deserts: Rolling Sand Dunes and Major Deserts (Thal Desert, Thar Desert, Cholistan Desert)
- Climatic Zones: Highland Climate, Lowland Climate, Coastal Climate, and Arid Climate zones
- Sources of Rainfall: Monsoon Rainfall, Western Depressions, Convectional Currents, and Relief Rainfall
- Environmental Hazards: Causes and Effects of Floods and Droughts in Pakistan
- Sources of Water: Groundwater and Surface Water Bodies (rivers, lakes, streams, aquifers)
- Water Issues: Waterlogging, Salinity, and Siltation problems and solutions
- Indus Waters Treaty: Background, provisions, and Impact on Pakistan's water resources
- Productive vs Protective Forests (types, uses, conservation, forestry management)
- Uses and Extraction of Oil and Natural Gas (extraction methods, uses, distribution)
- Non-renewable Power Resources: Coal, Petroleum, and Natural Gas (extraction, uses, reserves)
- Major Electricity Sources: Hydroelectric Power, Thermal Power, and Nuclear Energy
- Rural Electrification and Sustainable Development (rural energy access, sustainable energy)
- Agricultural Inputs: Natural (Soil, Rain) and Human (Capital, Machinery, HYV Seeds, fertilizers, pesticides)
- Food Crops: Wheat and Rice (Cultivation methods, Requirements, production, distribution)
- Cash Crops: Cotton and Sugar Cane (cultivation, processing, economic importance)
- Fruit: Mangoes, Bananas, and Apples (cultivation, distribution, export, varieties)
- Poultry Farming and Land Reforms (1959 Land Reforms, 1972 Land Reforms, 1977 Land Reforms)
- Oil Refineries: Major Locations (Karachi Refinery, Attock Refinery, Mehmood Kot Refinery)
- Formal vs Informal (Cottage) Sectors (industrial sectors, cottage industry, formal manufacturing)
- Industrial Estates in Sindh (SITE Karachi, Noorabad, Hub Industrial Estate)
- Export Processing Zones (EPZ) (locations, benefits, economic zones, special economic zones)
- Cultural, Archaeological, and Modern Attractions (tourist destinations, heritage sites, modern attractions)
- Role of Call Centres and Telecommunication (telecom industry, call centers, IT sector)
- Transportation: Roads (Kutcha vs Pucca Roads and the Role of NHA National Highway Authority)
- Transportation: Railways (Problems including Corruption, Worn-out Rails, and Consequences)
- Transportation: Dry Ports (Public vs Private Sector Dry Ports, locations, functions)
- Transportation: Seaports (Karachi Port, Port Qasim, and Gwadar Port, functions, importance)
- Transportation and Communication: Latitudes, Longitudes, and Neighboring Countries (geographic location, borders)
- Communication: Institutions (PTCL, PTA Pakistan Telecommunication Authority, NTC National Telecommunication Corporation) and Government Plans for E-Commerce
- Population Geography: Key Terms (Birth Rate, Death Rate, Growth Rate, and Density)
- Population Geography: Reasons for High Birth/Death Rates and Control Measures
- Population Geography: Rural-Urban Migration (Causes and Effects on Urban Infrastructure)
- Population Geography: Population Structure (Interpretation of Population Pyramids, age structure, dependency ratio)
- Imports, Exports, and Balance of Trade (trade patterns, balance of trade, major imports and exports)
- World Trade Organisation (WTO): Challenges for Pakistan (WTO membership, trade challenges, opportunities)
- Trading Blocs: SAARC and ECO (South Asian Association for Regional Cooperation, Economic Cooperation Organization)
- Irrigation: Need for Irrigation and Modern vs Conventional Methods (Karez system, Shaduf, Persian Wheels, modern irrigation)
- Water Infrastructure: Large vs Small Dams and Barrages (Tarbela, Mangla, Kalabagh, barrages, canal systems)
- Load Shedding: Causes (Siltation, Power Theft) and Economic Effects on industries and households
- Livestock Systems: Nomadic, Settled, and Transhumance/Semi-Nomadic systems (livestock rearing methods)""",
            
            "Islamiyat": """
- Theme 1 (Allah in Himself) • Passage 1 • Surah Al-Baqarah (2:255) – Ayat-ul-Kursi
- Theme 1 (Allah in Himself) • Passage 2 • Surah Al-An'aam (6:101–103)
- Theme 1 (Allah in Himself) • Passage 3 • Surah Fussilat (41:37)
- Theme 1 (Allah in Himself) • Passage 4 • Surah Shura (42:4–5)
- Theme 1 (Allah in Himself) • Passage 5 • Surah Ikhlas (112)
- Theme 2 (Created World) • Passage 6 • Surah Fatiha (1)
- Theme 2 (Created World) • Passage 7 • Surah Al-Baqarah (2:21–22)
- Theme 2 (Created World) • Passage 8 • Surah Al-Alaq (96:1–5)
- Theme 2 (Created World) • Passage 9 • Surah Az-Zilzaal (99)
- Theme 2 (Created World) • Passage 10 • Surah Naas (114)
- Theme 3 (Messengers) • Passage 11 • Surah Al-Baqarah (2:30–37) – Prophet Adam
- Theme 3 (Messengers) • Passage 12 • Surah Al-An'aam (6:75–79) – Prophet Ibrahim
- Theme 3 (Messengers) • Passage 13 • Surah Al-Maidah (5:110) – Prophet Isa
- Theme 3 (Messengers) • Passage 14 • Surah Duha (93) – Prophet Muhammad
- Theme 3 (Messengers) • Passage 15 • Surah Kauthar (108)
- Early life before prophethood (Prophet Muhammad's childhood, youth, character, life in Makkah before revelation)
- First revelation (revelation of first verses of Quran, experience of first revelation, beginning of prophethood)
- Makkan period (Prophet Muhammad's life in Makkah, early preaching, persecution, challenges, early converts)
- Opposition and persecution (opposition from Quraysh, persecution of early Muslims, boycott, hardships)
- Boycott of Banu Hashim (economic and social boycott, suffering, perseverance)
- Migration to Abyssinia (first migration, reasons, reception, return, second migration)
- Isra and Mi'raj (night journey and ascension, significance, spiritual experience, importance)
- Hijrah to Madinah (migration to Madinah, reasons, journey, significance, beginning of Islamic state)
- Madinan period: Leadership (Prophet Muhammad's leadership in Madinah, establishing Islamic state, governance)
- Madinan period: Battles (Battle of Badr, Battle of Uhud, Battle of Khandaq/Trench, Battle of Hunayn, key battles)
- Madinan period: Treaties (Treaty of Hudaybiyyah, treaties with various tribes, diplomatic relations)
- Madinan period: Conquest of Makkah (conquest of Makkah, events, significance, forgiveness)
- Madinan period: Farewell Sermon (final sermon, key teachings, completion of message)
- Ansar (helpers of Madinah, support for Muhajirun, role in Islamic community)
- Muhajirun (migrants from Makkah, companions who migrated, early Muslims)
- Ten Blessed Companions (Asharah Mubashsharah, ten companions promised Paradise, their lives and contributions)
- Character of the Prophet (Prophet Muhammad's character, attributes, exemplary conduct, sunnah)
- Importance of the Prophet as: Final Messenger (Prophet Muhammad as final messenger, seal of prophets, completion of message)
- Importance of the Prophet as: Role model (Prophet Muhammad as role model, exemplary behavior, following his example)
- Importance of the Prophet as: Leader (Prophet Muhammad's leadership, guidance, example in leadership)
- The Prophet's family (Prophet Muhammad's family, importance, respect, relationships)
- Wives (Mothers of the Believers, wives of Prophet Muhammad, their contributions, respect)
- Children (children of Prophet Muhammad, their lives, importance, respect)
- Grandchildren (grandchildren of Prophet Muhammad, Hasan and Husayn, their importance, respect)
- Descendants and Imams (Shi'a perspective) (descendants of Prophet Muhammad, Imams in Shi'a perspective, importance)
- Abu Bakr (RA) (first Caliph, life, contributions, caliphate, key achievements)
- Umar (RA) (second Caliph, life, contributions, caliphate, key achievements, expansion of Islamic state)
- Uthman (RA) (third Caliph, life, contributions, caliphate, compilation of Quran, key achievements)
- Ali (RA) (fourth Caliph, life, contributions, caliphate, key achievements, significance)
- Importance and contribution of the Companions (Sahabah, their role, contributions, significance, respect)
- Revelation of the Qur'an (revelation process, how Quran was revealed, methods of revelation, gradual revelation)
- Ways the Qur'an was revealed (different methods of revelation, gradual revelation, complete revelation)
- Scribes of the Qur'an (writers who recorded revelation, Zayd ibn Thabit, other scribes, their role)
- Preservation of the Qur'an (how Quran was preserved, compilation, standardization, protection from errors)
- Under Uthman (RA) (compilation and standardization of Quran, collection of Quranic manuscripts, distribution)
- During the Prophet's lifetime (recording of Quran during Prophet's time, memorization, writing, preservation)
- Under Abu Bakr (RA) (collection of Quran after Prophet's death, compilation efforts, Zayd ibn Thabit's role)
- Early transmission (early transmission of Quran, memorization, writing, oral tradition, verification)
- Qur'an as the primary source of Islamic law (Quran as primary legal source, legal principles, guidance for law)
- Importance of the Qur'an in Muslim life (significance of Quran, role in daily life, guidance, spiritual connection)
- Need for Hadith (why Hadith is needed, complementary role to Quran, explanation, practical guidance)
- Hadith (traditions of Prophet Muhammad, sayings and actions, importance, authenticity)
- Early transmission (early transmission of Hadith, memorization, oral tradition, verification)
- Compilation of Hadith (collection of Hadith, early compilations, major collections, efforts to preserve)
- Musnad and Musannaf collections (types of Hadith collections, organizational methods, major works)
- Authenticity of Hadith (methods to verify Hadith, isnad, matn, verification process, reliability)
- Isnad (chain of narrators, transmission chain, verification of authenticity, importance)
- Matn (text of Hadith, content verification, relationship with isnad, importance)
- Sunni and Shi'a Hadith collections (different collections, major works, differences, authenticity criteria)
- Hadith as a source of Islamic law (Hadith as legal source, relationship with Quran, role in jurisprudence)
- Importance of Hadith in Muslim life (significance of Hadith, role in daily life, guidance, following sunnah)
- Qur'an (as source of Islamic law, primary source, legal principles, guidance)
- Ijma (consensus of scholars, source of Islamic law, methods, importance, application)
- Qiyas (analogical reasoning, source of Islamic law, methods, importance, application)
- Sources of Islamic law (Quran, Hadith, Ijma, Qiyas, other sources, hierarchy, application)
- Individual conduct (personal behavior in Islam, ethics, character, moral values, Islamic ethics)
- Life in the community (community life in Islam, social responsibilities, relationships, Islamic social values)
- Major Teachings in the Hadiths (key teachings, moral lessons, guidance, practical application)
- Articles of Faith (six articles of faith, belief system, core beliefs, fundamentals of Islam)
- Belief in Allah (Tawheed, oneness of Allah, attributes, importance, significance)
- Angels (belief in angels, their roles, significance, importance in Islamic belief)
- Holy Books (belief in revealed books, previous scriptures, Quran, importance, preservation)
- Prophets (belief in prophets, previous prophets, Prophet Muhammad, importance, respect)
- Predestination (Qadr) (divine decree, free will, balance, understanding, significance)
- Resurrection and the Last Day (Day of Judgment, resurrection, accountability, afterlife, significance)
- Pillars of Islam (five pillars, fundamental practices, obligations, importance)
- Shahadah (declaration of faith, testimony, first pillar, significance, meaning)
- Salah (prayer, second pillar, importance, performance, times, significance)
- Zakah (charity, third pillar, obligatory charity, calculation, distribution, importance)
- Sawm (fasting, fourth pillar, Ramadan, importance, benefits, significance)
- Hajj (pilgrimage, fifth pillar, performance, significance, rites, importance)
- Jihad (all meanings) (struggle, different types, spiritual jihad, physical jihad, broader meaning, misconceptions)""",
            
            "Mathematics": """
- Algebra (equations, inequalities, functions, polynomials, quadratic equations, linear algebra, matrices, systems of equations)
- Geometry (shapes, angles, theorems, trigonometry, circles, polygons, coordinate geometry, transformations, Pythagorean theorem)
- Calculus (differentiation, integration, limits, series, derivatives, applications of calculus, optimization)
- Statistics and Probability (data analysis, distributions, hypothesis testing, probability theory, statistical inference, mean, median, mode)
- Number Theory (prime numbers, divisibility, modular arithmetic, number properties, sequences, series, Fibonacci)
- Discrete Mathematics (logic, sets, graphs, combinatorics, permutations, combinations, Boolean algebra)
- Applied Mathematics (modeling, optimization, numerical methods, real-world applications, mathematical modeling)""",
            
            "Physics": """
- Mechanics (kinematics, dynamics, work, energy, power, momentum, forces, motion, Newton's laws, projectile motion, circular motion)
- Thermodynamics (heat, temperature, entropy, laws of thermodynamics, heat engines, thermal properties, heat transfer, specific heat)
- Electromagnetism (electric fields, magnetic fields, circuits, waves, electromagnetic induction, AC/DC, Ohm's law, capacitance)
- Optics (light, reflection, refraction, lenses, mirrors, wave optics, optical instruments, interference, diffraction)
- Modern Physics (relativity, quantum mechanics, nuclear physics, atomic structure, particles, photoelectric effect, Bohr model)
- Waves and Sound (wave properties, sound intensity, Doppler effect, interference, resonance, standing waves, harmonics)
- Fluid Mechanics (fluid statics, fluid dynamics, buoyancy, pressure, flow, Bernoulli's principle, Archimedes' principle)""",
            
            "Chemistry": """
- Atomic Structure and Bonding (atoms, molecules, ions, chemical bonds, periodic table, electron configuration, valence electrons, Lewis structures)
- Chemical Reactions and Stoichiometry (balancing equations, reaction types, mole concept, limiting reactants, percent yield, empirical formulas)
- States of Matter (solids, liquids, gases, phase changes, intermolecular forces, properties of matter, phase diagrams, kinetic theory)
- Acids, Bases, and pH (acid-base theories, pH scale, titrations, buffers, neutralization, indicators, pH calculations)
- Organic Chemistry (hydrocarbons, functional groups, reaction mechanisms, organic compounds, polymers, alcohols, aldehydes, ketones)
- Electrochemistry (redox reactions, electrochemical cells, electrolysis, batteries, corrosion, oxidation numbers, half-reactions)
- Thermodynamics and Kinetics (energy changes, reaction rates, equilibrium, catalysts, Le Chatelier's principle, activation energy, enthalpy, entropy)"""
        }
        
        topics_list = subject_topics.get(subject_name, subject_topics["Business Studies"])
        
        # PHASE 3: Build prompt template (cacheable base - uses {{topic}} and {{learning_level}} placeholders)
        # The template is cached, then formatted with actual values when needed
        prompt_template = f"""You are an expert {subject_name} AI tutor helping a student
learn {subject_name} concepts related to {{topic}}.

🎯 YOUR PRIMARY GOAL:
Help students learn {subject_name} effectively. Be helpful, accurate, and context-aware.

📚 ALLOWED TOPICS ({subject_name}):
{topics_list}

⚠️ IMPORTANT CONTEXT AWARENESS:
- If a question mentions concepts that OVERLAP with {subject_name} (e.g., "demand" in Business Studies context, "geography" in historical context), it IS relevant to {subject_name}
- Questions that INTEGRATE {subject_name} with related fields (e.g., "how does economics affect business") ARE valid
- Questions that USE {subject_name} concepts to explain something ARE valid
- Only reject questions that are CLEARLY about completely unrelated subjects (e.g., "solve this math equation", "how to code", "write a poem")

❌ CLEARLY OFF-TOPIC (MUST REJECT):
- Pure math problem-solving (e.g., "solve this equation", "calculate the area of this triangle")
- Pure science experiments (e.g., "chemistry reaction", "physics experiment")
- Programming/coding (e.g., "how to code in Python", "write HTML")
- Literature/creative writing (e.g., "write a poem", "novel writing")
- Questions about COMPLETELY unrelated subjects with NO {subject_name} connection

✅ ACCEPT IF:
- Question relates to {subject_name} concepts, even if it mentions related fields
- Question asks about {subject_name} topics from the allowed list above
- Question integrates {subject_name} with relevant fields (e.g., economics in business context)
- Question uses {subject_name} terminology or concepts

REJECTION PROTOCOL (ONLY for CLEARLY off-topic questions):
If the question is CLEARLY about an unrelated subject with NO {subject_name} connection:
1. Politely decline: "I'm a {subject_name} tutor, so I focus on {subject_name} topics."
2. Offer alternatives: "I can help you with {subject_name} topics like [give 2-3 relevant examples]."
3. Encourage {subject_name} questions: "Feel free to ask me about [subject] concepts instead!"

YOUR TASK:
1. FIRST: Assess if the question relates to {subject_name} (be context-aware, not overly strict)
   - If it relates to {subject_name} or overlaps with {subject_name} concepts → ANSWER IT
   - If it's CLEARLY about an unrelated subject with NO connection → Politely redirect
2. Answer clearly and accurately using {subject_name} terminology
3. Use lesson content and related concepts when relevant
4. Match the student's learning level: {{learning_level}}
5. Provide examples where useful
6. Encourage follow-up questions about {subject_name}

REMEMBER: Be helpful and context-aware. If there's ANY connection to {subject_name}, answer the question.
"""
        
        # PHASE 3: Cache the template for future use
        _subject_prompt_cache[cache_key] = prompt_template
        
        return prompt_template

    def _generate_with_langchain(
        self,
        message: str,
        topic: str,
        learning_level: str,
        conversation_history: List[Dict],
        lesson_content: Optional[str] = None,
        concept_rows: Optional[List[Dict]] = None,
        explanation_style: str = "default",
        lesson_chunks: Optional[List[Dict]] = None,
        condensed_history: Optional[str] = None,
        student_profile: Optional[Dict] = None,
        subject_id: Optional[int] = None,
        subject_name: Optional[str] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> str:
        """Generate response using LangChain and gpt-4o-mini"""
        
        # Import performance instrumentation
        try:
            from services.performance_instrumentation import (
                time_prompt_construction,
                time_ai_call,
                time_response_parsing
            )
            INSTRUMENTATION_AVAILABLE = True
        except ImportError:
            INSTRUMENTATION_AVAILABLE = False
            # Create no-op context managers if instrumentation unavailable
            from contextlib import nullcontext
            time_prompt_construction = lambda *args, **kwargs: nullcontext()
            time_ai_call = lambda *args, **kwargs: nullcontext()
            time_response_parsing = lambda *args, **kwargs: nullcontext()

        # PHASE 1: Prompt Construction
        prompt_construction_start = time.time()
        with time_prompt_construction(
            stage_name="llm_service_prompt_construction",
            job_id=job_id,
            trace_id=trace_id
        ):
            # Use condensed history if provided, otherwise build from full history
            if condensed_history:
                history_text = condensed_history
            else:
                # Build history text from full conversation
                history_text = ""
                for m in conversation_history:
                    history_text += f"{m['role']}: {m['content']}\n"

            # Build concept summaries (OPTIMIZED: only names, no descriptions)
            concept_summaries = ""
            if concept_rows:
                # Only include concept names for speed (descriptions add tokens)
                names = [
                    c.get("name", "") for c in concept_rows[:5] if c.get("name")
                ]
                concept_summaries = ", ".join(names) if names else ""

            # OPTIMIZED: Skip student profile to reduce prompt size
            # Use default learning level from parameter instead
            profile_context = ""

            # Build lesson chunks text
            lesson_chunks_text = ""
            if lesson_chunks:
                lesson_chunks_text = "\n\n".join([
                    chunk.get("chunk_text", "")
                    for chunk in lesson_chunks
                    if chunk.get("chunk_text")
                ])

            # PHASE 3: Get subject-specific prompt (cached)
            subject_prompt_template = self._get_subject_prompt(
                subject_id, subject_name
            )
            
            # PHASE 3: Format template with actual values (faster than .format())
            # Use f-string for dynamic sections instead of .format() for better performance
            prompt = subject_prompt_template.replace("{{topic}}", topic).replace("{{learning_level}}", learning_level)
            
            # PHASE 3: Add context sections using f-strings (faster than concatenation)
            # Note: Off-topic detection is handled by ValidateInput node,
            # but prompt restriction is strong enough to enforce subject boundaries
            prompt += f"""
        {profile_context}
        ================================
        LESSON CONTENT
        ================================
        {lesson_content or "No lesson content was provided."}

        ================================
        MOST RELEVANT LESSON PASSAGES
        ================================
        {lesson_chunks_text or "No specific lesson passages found."}

        ================================
        KEY CONCEPTS (Supabase/pgvector search)
        ================================
        {concept_summaries or "No related concepts found."}

        ================================
        RECENT CONVERSATION (Supabase memory)
        ================================
        {history_text or "No prior messages."}

        ================================
        STUDENT QUESTION
        ================================
        {message}

        ================================
        ⚠️ SUBJECT RELEVANCE CHECK (Context-Aware) ⚠️
        ================================
        BEFORE answering, assess the question's relevance to {subject_name}:

        ✅ ACCEPT AND ANSWER IF:
        - Question is about {subject_name} topics from the allowed list
        - Question relates to {subject_name} concepts (even if it mentions related fields)
        - Question integrates {subject_name} with relevant areas (e.g., economics in business)
        - Question uses {subject_name} terminology or asks about {subject_name} applications

        ❌ REJECT (ONLY if CLEARLY off-topic):
        - Pure math problem-solving with NO {subject_name} context
        - Pure programming/coding questions
        - Literature/creative writing requests
        - Questions about COMPLETELY unrelated subjects with NO {subject_name} connection

        IMPORTANT: Be context-aware. If there's ANY connection to {subject_name}, answer it.
        Only reject questions that are CLEARLY about unrelated subjects.

        IF RELEVANT TO {subject_name}:
        → Answer the question clearly and comprehensively

        IF CLEARLY OFF-TOPIC (rare):
        → Use the polite rejection protocol from above

        ================================
        EXPLANATION FORMAT (Style Requested)
        ================================
        The student requested this explanation style: {explanation_style}

        Follow these rules:
        - If "simple": Give a clear, beginner-friendly explanation (2-3 paragraphs with examples).
        - If "detailed": Give a comprehensive, deep explanation with layered reasoning (4-6 paragraphs).
        - If "steps": Break the solution into clear numbered steps with explanations for each step.
        - If "table": Present the core explanation using a clean Markdown table with context.
        - If "diagram": Provide an ASCII diagram or conceptual sketch with detailed explanation.
        - If "comparison": Present a comparison chart of key differences with context and examples.
        - If "visual_prompt": Instead of explaining, output a prompt suitable
          for an image generation model (no more than 2–3 sentences).
        - If "default": Provide a comprehensive explanation (3-5 paragraphs) that fully addresses the question with examples, context, and relevant details to ensure understanding.

        Make sure the format is consistent with the requested style.

        ================================
        REASONING CLASSIFICATION (REQUIRED - DEEP ANALYSIS)
        ================================
        After providing your answer, perform a DEEP ANALYSIS of the student's question
        and classify their reasoning quality into ONE category: good, neutral, or confused.

        CRITICAL: Analyze the question THOROUGHLY before classifying:
        1. Examine the question's structure, complexity, and depth
        2. Identify the underlying reasoning, assumptions, and knowledge level
        3. Check for misconceptions, errors, or confusion indicators
        4. Assess the level of understanding demonstrated
        5. Consider the question's relationship to {subject_name} concepts

        Use these STRICT definitions with DEEP ANALYSIS:
        
        1. "good"
           The student demonstrates CLEAR UNDERSTANDING and SOPHISTICATED REASONING.
           Deep analysis indicators:
           • Uses correct {subject_name} terminology accurately
           • Applies concepts correctly to new situations
           • Makes logical connections between related concepts
           • Asks analytical, evaluative, or synthesis-level questions
           • Demonstrates higher-order thinking (analysis, evaluation, creation)
           • Shows ability to compare, contrast, or integrate concepts
           • Builds on prior knowledge accurately
           • Asks "why" or "how" questions that show deep thinking
           • Proposes hypotheses or explores implications
           • Uses subject-specific vocabulary correctly
           
           Examples of "good" reasoning:
           - "How does price elasticity affect total revenue in different market conditions?"
           - "What's the difference between X and Y, and when would you use each?"
           - "If this happens, what would be the implications for...?"
        
        2. "neutral"
           The student asks STANDARD QUESTIONS without showing clear understanding
           or misunderstanding. This is the DEFAULT category when uncertain.
           Deep analysis indicators:
           • Simple factual or definition requests
           • Basic "what is" or "explain" questions
           • Standard textbook-level inquiries
           • No evidence of deep understanding OR clear misunderstanding
           • Seeking basic information or clarification
           • Questions that could be answered by looking up definitions
           • No application, analysis, or evaluation demonstrated
           
           Examples of "neutral" reasoning:
           - "What is [concept]?"
           - "Can you explain [topic]?"
           - "I need help with [basic topic]"
        
        3. "confused"
           The student shows CLEAR MISUNDERSTANDING, INCORRECT REASONING, or
           FUNDAMENTAL MISCONCEPTIONS. This requires STRONG EVIDENCE of confusion.
           Deep analysis indicators:
           • States incorrect facts or definitions
           • Confuses related but distinct concepts
           • Shows logical contradictions or inconsistencies
           • Misapplies concepts to wrong contexts
           • Uses terminology incorrectly
           • Asks questions that reveal fundamental gaps in understanding
           • Confuses cause and effect relationships
           • Mixes up unrelated concepts
           • Shows persistent misconceptions despite context
           
           Examples of "confused" reasoning:
           - "Is X the same as Y?" (when they're clearly different)
           - "Does X mean Y?" (when X means something else entirely)
           - Statements that contradict established {subject_name} principles
           - Questions that reveal fundamental misunderstanding of core concepts

        CLASSIFICATION PROCESS:
        1. Read the student's question CAREFULLY
        2. Analyze the UNDERLYING REASONING and KNOWLEDGE LEVEL
        3. Look for STRONG INDICATORS of understanding, neutrality, or confusion
        4. When in doubt between "neutral" and "confused", choose "neutral"
        5. Only classify as "confused" if there is CLEAR EVIDENCE of misunderstanding
        6. Only classify as "good" if there is CLEAR EVIDENCE of sophisticated reasoning

        IMPORTANT: Be CONSERVATIVE - default to "neutral" unless there is STRONG
        evidence for "good" or "confused". Misclassification can affect learning outcomes.

        ================================
        FINAL REMINDER
        ================================
        You are a {subject_name} tutor. Be helpful, comprehensive, and context-aware.
        - Provide detailed, thorough explanations that fully address the student's question
        - Include examples, context, and relevant details to ensure understanding
        - If the question relates to {subject_name} (even with overlaps) → ANSWER IT COMPREHENSIVELY
        - Only reject if CLEARLY about unrelated subjects with NO {subject_name} connection
        - Use your judgment to assess relevance, don't be overly strict
        
        IMPORTANT: Return your response in the following JSON format:
        {{
            "response": "<your detailed, comprehensive answer to the student's question - be thorough and include examples>",
            "reasoning_label": "<good|neutral|confused>"
        }}
        """
            prompt_size = len(prompt)
        
        # METRICS: Track prompt construction time (non-blocking, failure-safe)
        prompt_construction_duration_ms = (time.time() - prompt_construction_start) * 1000
        if METRICS_AVAILABLE and metrics_service:
            try:
                metrics_service.track_ai_call_duration(
                    agent_name="tutor",
                    duration_ms=prompt_construction_duration_ms,
                    call_type="prompt_construction",
                    model="gpt-4o-mini",
                    prompt_tokens=prompt_size // 4,  # Rough estimate
                    job_id=job_id
                )
            except Exception:
                pass  # Non-blocking

        # PHASE 2: API Call
        # Add timeout protection for LLM invoke (30 seconds)
        import threading
        result_container = {"value": None, "error": None, "completed": False}
        
        # Track AI call start time for metrics
        ai_call_start_time = time.time()
        
        def invoke_llm():
            try:
                result_container["value"] = self.llm.invoke(prompt)
                result_container["completed"] = True
            except Exception as e:
                result_container["error"] = e
                result_container["completed"] = True
        
        with time_ai_call(
            stage_name="llm_service_api_call",
            job_id=job_id,
            trace_id=trace_id,
            model="gpt-4o-mini",
            prompt_tokens=prompt_size // 4  # Rough estimate
        ):
            invoke_thread = threading.Thread(target=invoke_llm, daemon=True)
            invoke_thread.start()
            invoke_thread.join(timeout=30)
        
        # Calculate AI call duration for metrics
        ai_call_duration_ms = (time.time() - ai_call_start_time) * 1000
        
        if not result_container["completed"]:
            # METRICS: Track timeout AI call (non-blocking, failure-safe)
            if METRICS_AVAILABLE and metrics_service:
                try:
                    metrics_service.track_ai_call_duration(
                        agent_name="tutor",
                        duration_ms=ai_call_duration_ms,
                        call_type="api_call",
                        model="gpt-4o-mini",
                        job_id=job_id
                    )
                except Exception:
                    pass  # Non-blocking
            raise TimeoutError("LLM invoke timed out after 30 seconds")
        
        if result_container["error"]:
            # METRICS: Track failed AI call (non-blocking, failure-safe)
            if METRICS_AVAILABLE and metrics_service:
                try:
                    metrics_service.track_ai_call_duration(
                        agent_name="tutor",
                        duration_ms=ai_call_duration_ms,
                        call_type="api_call",
                        model="gpt-4o-mini",
                        job_id=job_id
                    )
                except Exception:
                    pass  # Non-blocking
            raise result_container["error"]
        
        response = result_container["value"]

        # Extract token usage from response metadata
        token_usage = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }

        if hasattr(response, 'response_metadata'):
            metadata = response.response_metadata
            if metadata and 'token_usage' in metadata:
                usage = metadata['token_usage']
                token_usage = {
                    "prompt_tokens": usage.get('prompt_tokens', 0),
                    "completion_tokens": usage.get('completion_tokens', 0),
                    "total_tokens": usage.get('total_tokens', 0)
                }
        
        # METRICS: Track successful AI call duration (non-blocking, failure-safe)
        if METRICS_AVAILABLE and metrics_service:
            try:
                metrics_service.track_ai_call_duration(
                    agent_name="tutor",
                    duration_ms=ai_call_duration_ms,
                    call_type="api_call",
                    model="gpt-4o-mini",
                    prompt_tokens=token_usage.get('prompt_tokens', 0),
                    completion_tokens=token_usage.get('completion_tokens', 0),
                    job_id=job_id
                )
            except Exception as e:
                # Non-blocking: log but don't fail response
                logger.debug(f"Failed to track AI call duration: {e}")

        # PHASE 3: Response Parsing and Validation
        with time_response_parsing(
            stage_name="llm_service_response_parsing",
            job_id=job_id,
            trace_id=trace_id,
            response_size=token_usage.get('completion_tokens')
        ):
            # Parse structured output (response + reasoning_label)
            response_content = response.content.strip()
            reasoning_label = "neutral"  # Default fallback
            
            # Try to parse JSON from response
            try:
                json_start = response_content.find('{')
                json_end = response_content.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    json_str = response_content[json_start:json_end]
                    parsed_data = json.loads(json_str)
                    
                    # Extract reasoning_label if present
                    if "reasoning_label" in parsed_data:
                        reasoning_label = parsed_data["reasoning_label"].lower()
                        # Validate reasoning_label
                        if reasoning_label not in ["good", "neutral", "confused"]:
                            reasoning_label = "neutral"
                        # Extract response if structured
                        if "response" in parsed_data:
                            response_content = parsed_data["response"]
            except (json.JSONDecodeError, KeyError, ValueError):
                # If JSON parsing fails, use response as-is and keep default reasoning_label
                pass
            
            # CRITICAL FIX: Remove any reasoning classification text that might leak into response
            # This ensures "Reasoning Classification: neutral" or similar text doesn't appear in the response
            import re
            # Remove patterns like "Reasoning Classification: neutral", "Reasoning: neutral", etc.
            # Also catch patterns like "**Reasoning Classification:**" with markdown formatting
            response_content = re.sub(
                r'(?i)(\*\*)?reasoning\s*(classification|label|category)?\s*:?\s*\*?.*?(good|neutral|confused).*?',
                '',
                response_content,
                flags=re.DOTALL
            )
            # Remove any section that starts with "Reasoning Classification" and continues until end or next section
            response_content = re.sub(
                r'(?i)(\*\*)?reasoning\s*(classification|label|category)?\s*:?\s*\*?.*?(\n\n|\Z)',
                '',
                response_content,
                flags=re.DOTALL
            )
            # Remove patterns like "I would classify" or "Classification:" followed by reasoning labels
            response_content = re.sub(
                r'(?i)(I\s+would\s+classify|classification\s+of).*?(good|neutral|confused).*?(\n\n|\Z)',
                '',
                response_content,
                flags=re.DOTALL
            )
            # Clean up extra whitespace and newlines
            response_content = re.sub(r'\n{3,}', '\n\n', response_content)  # Max 2 consecutive newlines
            response_content = response_content.strip()

        return (response_content, token_usage, reasoning_label)

    async def generate_reply_async(
        self,
        message: str,
        topic: str,
        learning_level: str,
        conversation_history: List[Dict],
        lesson_content: Optional[str] = None,
        concept_rows: Optional[List[Dict]] = None,
        explanation_style: str = "default",
        lesson_chunks: Optional[List[Dict]] = None,
        condensed_history: Optional[str] = None,
        student_profile: Optional[Dict] = None,
        subject_id: Optional[int] = None,
        subject_name: Optional[str] = None,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ) -> tuple:
        """
        PHASE 2: Async version of generate_reply for better concurrency.
        Uses native async LLM calls instead of threading.
        """
        if not self.langchain_available or not self.llm:
            return self._generate_fallback_response(message, topic)
        
        # Import performance instrumentation
        try:
            from services.performance_instrumentation import (
                time_prompt_construction,
                time_ai_call,
                time_response_parsing
            )
            INSTRUMENTATION_AVAILABLE = True
        except ImportError:
            INSTRUMENTATION_AVAILABLE = False
            from contextlib import nullcontext
            time_prompt_construction = lambda *args, **kwargs: nullcontext()
            time_ai_call = lambda *args, **kwargs: nullcontext()
            time_response_parsing = lambda *args, **kwargs: nullcontext()

        # PHASE 1: Prompt Construction (same as sync version)
        prompt_construction_start = time.time()
        with time_prompt_construction(
            stage_name="llm_service_prompt_construction",
            job_id=job_id,
            trace_id=trace_id
        ):
            if condensed_history:
                history_text = condensed_history
            else:
                history_text = ""
                for m in conversation_history:
                    history_text += f"{m['role']}: {m['content']}\n"

            concept_summaries = ""
            if concept_rows:
                names = [
                    c.get("name", "") for c in concept_rows[:5] if c.get("name")
                ]
                concept_summaries = ", ".join(names) if names else ""

            profile_context = ""
            lesson_chunks_text = ""
            if lesson_chunks:
                lesson_chunks_text = "\n\n".join([
                    chunk.get("chunk_text", "")
                    for chunk in lesson_chunks
                    if chunk.get("chunk_text")
                ])

            subject_prompt_template = self._get_subject_prompt(
                subject_id, subject_name
            )
            
            # PHASE 3: Format template with actual values (faster than .format())
            prompt = subject_prompt_template.replace("{{topic}}", topic).replace("{{learning_level}}", learning_level)
            
            prompt += f"""
        {profile_context}
        ================================
        LESSON CONTENT
        ================================
        {lesson_content or "No lesson content was provided."}

        ================================
        MOST RELEVANT LESSON PASSAGES
        ================================
        {lesson_chunks_text or "No specific lesson passages found."}

        ================================
        KEY CONCEPTS (Supabase/pgvector search)
        ================================
        {concept_summaries or "No related concepts found."}

        ================================
        RECENT CONVERSATION (Supabase memory)
        ================================
        {history_text or "No prior messages."}

        ================================
        STUDENT QUESTION
        ================================
        {message}

        ================================
        ⚠️ SUBJECT RELEVANCE CHECK (Context-Aware) ⚠️
        ================================
        BEFORE answering, assess the question's relevance to {subject_name}:

        ✅ ACCEPT AND ANSWER IF:
        - Question is about {subject_name} topics from the allowed list
        - Question relates to {subject_name} concepts (even if it mentions related fields)
        - Question integrates {subject_name} with relevant areas (e.g., economics in business)
        - Question uses {subject_name} terminology or asks about {subject_name} applications

        ❌ REJECT (ONLY if CLEARLY off-topic):
        - Pure math problem-solving with NO {subject_name} context
        - Pure programming/coding questions
        - Literature/creative writing requests
        - Questions about COMPLETELY unrelated subjects with NO {subject_name} connection

        IMPORTANT: Be context-aware. If there's ANY connection to {subject_name}, answer it.
        Only reject questions that are CLEARLY about unrelated subjects.

        IF RELEVANT TO {subject_name}:
        → Answer the question clearly and comprehensively

        IF CLEARLY OFF-TOPIC (rare):
        → Use the polite rejection protocol from above

        ================================
        EXPLANATION FORMAT (Style Requested)
        ================================
        The student requested this explanation style: {explanation_style}

        Follow these rules:
        - If "simple": Give a clear, beginner-friendly explanation (2-3 paragraphs with examples).
        - If "detailed": Give a comprehensive, deep explanation with layered reasoning (4-6 paragraphs).
        - If "steps": Break the solution into clear numbered steps with explanations for each step.
        - If "table": Present the core explanation using a clean Markdown table with context.
        - If "diagram": Provide an ASCII diagram or conceptual sketch with detailed explanation.
        - If "comparison": Present a comparison chart of key differences with context and examples.
        - If "visual_prompt": Instead of explaining, output a prompt suitable
          for an image generation model (no more than 2–3 sentences).
        - If "default": Provide a comprehensive explanation (3-5 paragraphs) that fully addresses the question with examples, context, and relevant details to ensure understanding.

        Make sure the format is consistent with the requested style.

        ================================
        REASONING CLASSIFICATION (REQUIRED - DEEP ANALYSIS)
        ================================
        After providing your answer, perform a DEEP ANALYSIS of the student's question
        and classify their reasoning quality into ONE category: good, neutral, or confused.

        CRITICAL: Analyze the question THOROUGHLY before classifying:
        1. Examine the question's structure, complexity, and depth
        2. Identify the underlying reasoning, assumptions, and knowledge level
        3. Check for misconceptions, errors, or confusion indicators
        4. Assess the level of understanding demonstrated
        5. Consider the question's relationship to {subject_name} concepts

        Use these STRICT definitions with DEEP ANALYSIS:
        
        1. "good"
           The student demonstrates CLEAR UNDERSTANDING and SOPHISTICATED REASONING.
           Deep analysis indicators:
           • Uses correct {subject_name} terminology accurately
           • Applies concepts correctly to new situations
           • Makes logical connections between related concepts
           • Asks analytical, evaluative, or synthesis-level questions
           • Demonstrates higher-order thinking (analysis, evaluation, creation)
           • Shows ability to compare, contrast, or integrate concepts
           • Builds on prior knowledge accurately
           • Asks "why" or "how" questions that show deep thinking
           • Proposes hypotheses or explores implications
           • Uses subject-specific vocabulary correctly
           
           Examples of "good" reasoning:
           - "How does price elasticity affect total revenue in different market conditions?"
           - "What's the difference between X and Y, and when would you use each?"
           - "If this happens, what would be the implications for...?"
        
        2. "neutral"
           The student asks STANDARD QUESTIONS without showing clear understanding
           or misunderstanding. This is the DEFAULT category when uncertain.
           Deep analysis indicators:
           • Simple factual or definition requests
           • Basic "what is" or "explain" questions
           • Standard textbook-level inquiries
           • No evidence of deep understanding OR clear misunderstanding
           • Seeking basic information or clarification
           • Questions that could be answered by looking up definitions
           • No application, analysis, or evaluation demonstrated
           
           Examples of "neutral" reasoning:
           - "What is [concept]?"
           - "Can you explain [topic]?"
           - "I need help with [basic topic]"
        
        3. "confused"
           The student shows CLEAR MISUNDERSTANDING, INCORRECT REASONING, or
           FUNDAMENTAL MISCONCEPTIONS. This requires STRONG EVIDENCE of confusion.
           Deep analysis indicators:
           • States incorrect facts or definitions
           • Confuses related but distinct concepts
           • Shows logical contradictions or inconsistencies
           • Misapplies concepts to wrong contexts
           • Uses terminology incorrectly
           • Asks questions that reveal fundamental gaps in understanding
           • Confuses cause and effect relationships
           • Mixes up unrelated concepts
           • Shows persistent misconceptions despite context
           
           Examples of "confused" reasoning:
           - "Is X the same as Y?" (when they're clearly different)
           - "Does X mean Y?" (when X means something else entirely)
           - Statements that contradict established {subject_name} principles
           - Questions that reveal fundamental misunderstanding of core concepts

        CLASSIFICATION PROCESS:
        1. Read the student's question CAREFULLY
        2. Analyze the UNDERLYING REASONING and KNOWLEDGE LEVEL
        3. Look for STRONG INDICATORS of understanding, neutrality, or confusion
        4. When in doubt between "neutral" and "confused", choose "neutral"
        5. Only classify as "confused" if there is CLEAR EVIDENCE of misunderstanding
        6. Only classify as "good" if there is CLEAR EVIDENCE of sophisticated reasoning

        IMPORTANT: Be CONSERVATIVE - default to "neutral" unless there is STRONG
        evidence for "good" or "confused". Misclassification can affect learning outcomes.

        ================================
        FINAL REMINDER
        ================================
        You are a {subject_name} tutor. Be helpful, comprehensive, and context-aware.
        - Provide detailed, thorough explanations that fully address the student's question
        - Include examples, context, and relevant details to ensure understanding
        - If the question relates to {subject_name} (even with overlaps) → ANSWER IT COMPREHENSIVELY
        - Only reject if CLEARLY about unrelated subjects with NO {subject_name} connection
        - Use your judgment to assess relevance, don't be overly strict
        
        IMPORTANT: Return your response in the following JSON format:
        {{
            "response": "<your detailed, comprehensive answer to the student's question - be thorough and include examples>",
            "reasoning_label": "<good|neutral|confused>"
        }}
        """
            prompt_size = len(prompt)
        
        prompt_construction_duration_ms = (time.time() - prompt_construction_start) * 1000
        if METRICS_AVAILABLE and metrics_service:
            try:
                metrics_service.track_ai_call_duration(
                    agent_name="tutor",
                    duration_ms=prompt_construction_duration_ms,
                    call_type="prompt_construction",
                    model="gpt-4o-mini",
                    prompt_tokens=prompt_size // 4,
                    job_id=job_id
                )
            except Exception:
                pass

        # PHASE 2: Async API Call (native async, no threading overhead)
        ai_call_start_time = time.time()
        
        with time_ai_call(
            stage_name="llm_service_api_call",
            job_id=job_id,
            trace_id=trace_id,
            model="gpt-4o-mini",
            prompt_tokens=prompt_size // 4
        ):
            try:
                # PHASE 2: Use native async LLM call with timeout
                import asyncio
                response = await asyncio.wait_for(
                    self.llm.ainvoke(prompt),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                ai_call_duration_ms = (time.time() - ai_call_start_time) * 1000
                if METRICS_AVAILABLE and metrics_service:
                    try:
                        metrics_service.track_ai_call_duration(
                            agent_name="tutor",
                            duration_ms=ai_call_duration_ms,
                            call_type="api_call",
                            model="gpt-4o-mini",
                            job_id=job_id
                        )
                    except Exception:
                        pass
                raise TimeoutError("LLM invoke timed out after 30 seconds")
            except Exception as e:
                ai_call_duration_ms = (time.time() - ai_call_start_time) * 1000
                if METRICS_AVAILABLE and metrics_service:
                    try:
                        metrics_service.track_ai_call_duration(
                            agent_name="tutor",
                            duration_ms=ai_call_duration_ms,
                            call_type="api_call",
                            model="gpt-4o-mini",
                            job_id=job_id
                        )
                    except Exception:
                        pass
                raise

        ai_call_duration_ms = (time.time() - ai_call_start_time) * 1000

        # Extract token usage
        token_usage = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }

        if hasattr(response, 'response_metadata'):
            metadata = response.response_metadata
            if metadata and 'token_usage' in metadata:
                usage = metadata['token_usage']
                token_usage = {
                    "prompt_tokens": usage.get('prompt_tokens', 0),
                    "completion_tokens": usage.get('completion_tokens', 0),
                    "total_tokens": usage.get('total_tokens', 0)
                }
        
        if METRICS_AVAILABLE and metrics_service:
            try:
                metrics_service.track_ai_call_duration(
                    agent_name="tutor",
                    duration_ms=ai_call_duration_ms,
                    call_type="api_call",
                    model="gpt-4o-mini",
                    prompt_tokens=token_usage.get('prompt_tokens', 0),
                    completion_tokens=token_usage.get('completion_tokens', 0),
                    job_id=job_id
                )
            except Exception as e:
                logger.debug(f"Failed to track AI call duration: {e}")

        # PHASE 3: Response Parsing
        with time_response_parsing(
            stage_name="llm_service_response_parsing",
            job_id=job_id,
            trace_id=trace_id,
            response_size=token_usage.get('completion_tokens')
        ):
            response_content = response.content.strip()
            reasoning_label = "neutral"
            
            try:
                json_start = response_content.find('{')
                json_end = response_content.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    json_str = response_content[json_start:json_end]
                    parsed_data = json.loads(json_str)
                    
                    if "reasoning_label" in parsed_data:
                        reasoning_label = parsed_data["reasoning_label"].lower()
                        if reasoning_label not in ["good", "neutral", "confused"]:
                            reasoning_label = "neutral"
                        if "response" in parsed_data:
                            response_content = parsed_data["response"]
            except (json.JSONDecodeError, KeyError, ValueError):
                pass
            
            # CRITICAL FIX: Remove any reasoning classification text that might leak into response
            # This ensures "Reasoning Classification: neutral" or similar text doesn't appear in the response
            import re
            # Remove patterns like "Reasoning Classification: neutral", "Reasoning: neutral", etc.
            # Also catch patterns like "**Reasoning Classification:**" with markdown formatting
            response_content = re.sub(
                r'(?i)(\*\*)?reasoning\s*(classification|label|category)?\s*:?\s*\*?.*?(good|neutral|confused).*?',
                '',
                response_content,
                flags=re.DOTALL
            )
            # Remove any section that starts with "Reasoning Classification" and continues until end or next section
            response_content = re.sub(
                r'(?i)(\*\*)?reasoning\s*(classification|label|category)?\s*:?\s*\*?.*?(\n\n|\Z)',
                '',
                response_content,
                flags=re.DOTALL
            )
            # Remove patterns like "I would classify" or "Classification:" followed by reasoning labels
            response_content = re.sub(
                r'(?i)(I\s+would\s+classify|classification\s+of).*?(good|neutral|confused).*?(\n\n|\Z)',
                '',
                response_content,
                flags=re.DOTALL
            )
            # Clean up extra whitespace and newlines
            response_content = re.sub(r'\n{3,}', '\n\n', response_content)  # Max 2 consecutive newlines
            response_content = response_content.strip()

        return (response_content, token_usage, reasoning_label)

    def trim_context(
        self,
        history: List[Dict],
        lesson_text: Optional[str],
        chunks: Optional[List[Dict]],
        max_tokens: int = 6000
    ) -> tuple:
        """
        Trim context to fit within token budget.

        Args:
            history: Conversation history list
            lesson_text: Optional lesson content
            chunks: Optional lesson chunks
            max_tokens: Maximum token budget (default: 6000)

        Returns:
            tuple: (trimmed_history, trimmed_lesson_text, trimmed_chunks)
        """
        # Estimate tokens (rough: 1 token ≈ 4 characters)
        def estimate_tokens(text: str) -> int:
            return len(text) // 4 if text else 0

        # Calculate current token usage
        history_tokens = sum(
            estimate_tokens(msg.get("content", "")) for msg in history
        )
        lesson_tokens = estimate_tokens(lesson_text or "")
        chunks_tokens = sum(
            estimate_tokens(chunk.get("chunk_text", ""))
            for chunk in (chunks or [])
        )

        total_tokens = history_tokens + lesson_tokens + chunks_tokens

        # If within budget, return as-is
        if total_tokens <= max_tokens:
            return history, lesson_text, chunks

        # Need to trim - prioritize keeping lesson and chunks, trim history
        available_for_history = max_tokens - lesson_tokens - chunks_tokens
        available_for_history = max(0, available_for_history)

        # Trim oldest messages from history
        trimmed_history = []
        current_tokens = 0
        # Iterate in reverse to keep newest messages
        for msg in reversed(history):
            msg_tokens = estimate_tokens(msg.get("content", ""))
            if current_tokens + msg_tokens <= available_for_history:
                trimmed_history.insert(0, msg)
                current_tokens += msg_tokens
            else:
                # Can't fit this message, stop
                break

        # Only log in debug mode
        if os.getenv("DEBUG", "0") == "1":
            self.logger.info(
                f"Trimmed context: {len(history)} -> {len(trimmed_history)} "
                f"messages ({total_tokens} -> "
                f"{current_tokens + lesson_tokens + chunks_tokens} tokens)"
            )

        return trimmed_history, lesson_text, chunks

    def _get_subject_name_from_id(self, subject_id: Optional[int]) -> str:
        """Get subject name from subject_id"""
        subject_map = {
            101: "Business Studies",
            102: "Islamiyat",
            113: "Pak Studies Geography",
            114: "Pak Studies History",
            119: "Economics",
            103: "Mathematics",
            104: "Physics",
            105: "Chemistry"
        }
        return subject_map.get(subject_id, "Business Studies")
    
    def get_off_topic_keywords(self, subject_id: Optional[int]) -> List[str]:
        """Get off-topic keywords based on current subject
        
        IMPORTANT: Only flag obvious off-topic questions using phrase matching.
        Uses context-aware detection to avoid false positives.
        """
        # Common off-topic keywords for ALL subjects (obvious non-academic)
        common_off_topic = [
            "write code", "programming language", "python script", "javascript code",
            "html page", "css styling", "write a poem", "poetry writing",
            "novel writing", "translate to", "translate from", "grammar rules",
            "vocabulary words", "spell this", "how to code", "programming tutorial"
        ]
        
        if subject_id == 101:  # Business Studies
            # REMOVED overlapping keywords like "economics", "demand", "supply" - these ARE relevant!
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate the area",
                "find the derivative", "solve for x", "algebra problem", "geometry proof",
                "calculus problem", "physics experiment", "chemistry reaction",
                "biology experiment", "science lab", "history of ancient",
                "world war details", "medieval period", "pakistan independence movement",
                "pakistan geography", "physical geography", "islamiyat topics",
                "quranic verses", "hadith interpretation", "islamic law details"
            ]
        elif subject_id == 119:  # Economics
            # REMOVED overlapping keywords - business concepts ARE relevant to Economics!
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate the area",
                "find the derivative", "solve for x", "algebra problem", "geometry proof",
                "calculus problem", "physics experiment", "chemistry reaction",
                "biology experiment", "science lab", "pakistan independence",
                "muhammad ali jinnah", "pakistan geography", "physical geography",
                "mountains of pakistan", "rivers of pakistan", "islamiyat topics",
                "quranic verses", "hadith interpretation", "islamic law details"
            ]
        elif subject_id == 114:  # Pak Studies History
            # REMOVED overlapping keywords - geography can be relevant in historical context!
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate",
                "algebra", "geometry", "calculus", "physics experiment",
                "chemistry reaction", "biology experiment", "pure geography",
                "geography of other countries", "physical geography details",
                "business management", "marketing strategies", "economics calculations",
                "islamiyat topics", "quranic verses", "hadith interpretation",
                "islamic law details", "religious rulings"
            ]
        elif subject_id == 113:  # Pak Studies Geography
            # REMOVED overlapping keywords - history can be relevant in geographic context!
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate",
                "algebra", "geometry", "calculus", "physics experiment",
                "chemistry reaction", "biology experiment", "pure history",
                "history of other countries", "ancient history", "medieval history",
                "world war details", "business management", "marketing strategies",
                "islamiyat topics", "quranic verses", "hadith interpretation",
                "islamic law details", "religious rulings"
            ]
        elif subject_id == 102:  # Islamiyat
            # REMOVED overlapping keywords - history/geography can be relevant in Islamic context!
            return common_off_topic + [
                "solve this math problem", "solve equation", "calculate",
                "algebra", "geometry", "calculus", "physics experiment",
                "chemistry reaction", "biology experiment", "pure geography",
                "geography of other countries", "business management",
                "marketing strategies", "economics calculations", "pakistan independence movement",
                "muhammad ali jinnah biography"  # Only specific non-Islamic history
            ]
        elif subject_id == 103:  # Mathematics
            return common_off_topic + [
                "history of", "pakistan history", "geography", "business",
                "marketing", "economics concepts", "islamiyat", "physics experiment",
                "chemistry reaction", "biology experiment"
            ]
        elif subject_id == 104:  # Physics
            return common_off_topic + [
                "history of", "pakistan history", "geography", "business",
                "marketing", "economics", "islamiyat", "chemistry reaction",
                "biology experiment", "solve math", "algebra problem"
            ]
        elif subject_id == 105:  # Chemistry
            return common_off_topic + [
                "history of", "pakistan history", "geography", "business",
                "marketing", "economics", "islamiyat", "physics experiment",
                "biology experiment", "solve math", "algebra problem"
            ]
        else:
            # Default: reject only obvious non-academic topics
            return common_off_topic + [
                "solve this math problem", "write code", "programming"
            ]
    
    def fallback_reply(self, message: str, topic: str, subject_name: Optional[str] = None) -> str:
        """
        Generate a safe fallback response when LLM generation fails.

        Args:
            message: Student's message/question
            topic: Topic/subject
            subject_name: Optional subject name

        Returns:
            str: Safe fallback message
        """
        if subject_name:
            return f"Let's approach this {subject_name} question step-by-step."
        return "Let's approach this step-by-step."

    def essay_marker(
        self,
        essay_text: str,
        topic: str,
        user_id: Optional[str] = None
    ) -> Dict:
        """
        Mark/grade an essay submission.

        TODO: Full implementation pending.
        This will provide detailed feedback, scoring, and suggestions.

        Args:
            essay_text: The student's essay text
            topic: Topic/subject of the essay
            user_id: Optional user ID for personalization

        Returns:
            Dict with:
            {
                "feedback": str,
                "score": float (0-100),
                "suggestions": List[str]
            }
        """
        # TODO: Implement full essay marking logic
        # For now, return placeholder
        self.logger.warning(
            "[TODO] essay_marker() not yet fully implemented"
        )
        return {
            "feedback": (
                "Essay marking is not yet fully implemented. "
                "This will provide detailed feedback, scoring, "
                "and suggestions in the future."
            ),
            "score": 0.0,
            "suggestions": []
        }

    def _generate_fallback_response(
        self, message: str, topic: str, subject_name: Optional[str] = None
    ) -> str:
        """Generate fallback response when LangChain is unavailable"""
        if subject_name:
            return (
                f"I'm here to help you with {subject_name}, specifically {topic}! "
                f"Your question: '{message}' is important. "
                "Let me provide you with a comprehensive explanation..."
            )
        return (
            f"I'm here to help you with {topic}! "
            f"Your question: '{message}' is important. "
            "Let me provide you with a comprehensive explanation..."
        )

    def generate_lesson(
        self,
        topic: str,
        learning_objectives: List[str],
        difficulty_level: str = "intermediate"
    ):
        """
        Generate a structured lesson. Uses LangChain if available,
        otherwise fallback.

        Args:
            topic: Lesson topic
            learning_objectives: List of learning objectives
            difficulty_level: Difficulty level

        Returns:
            LessonResponse object
        """
        if self.langchain_available and self.llm:
            try:
                return self._generate_lesson_with_langchain(
                    topic, learning_objectives, difficulty_level
                )
            except Exception:
                return self._generate_fallback_lesson(
                    topic, learning_objectives, difficulty_level
                )
        else:
            return self._generate_fallback_lesson(
                topic, learning_objectives, difficulty_level
            )

    def _generate_lesson_with_langchain(
        self,
        topic: str,
        learning_objectives: List[str],
        difficulty_level: str
    ):
        """Generate structured lesson using LangChain"""

        prompt = f"""
        Create a comprehensive lesson on {topic} with the following
        learning objectives:
        {', '.join(learning_objectives)}

        Difficulty level: {difficulty_level}

        Provide:
        1. Lesson content (detailed explanation)
        2. Key points (bullet points)
        3. Practice questions (3-5 questions)
        4. Estimated duration in minutes

        Format as JSON:
        {{
            "lesson_content": "...",
            "key_points": ["...", "..."],
            "practice_questions": ["...", "..."],
            "estimated_duration": 30
        }}
        """

        # Add timeout protection for LLM invoke (30 seconds)
        import threading
        result_container = {"value": None, "error": None, "completed": False}
        
        def invoke_llm():
            try:
                result_container["value"] = self.llm.invoke(prompt)
                result_container["completed"] = True
            except Exception as e:
                result_container["error"] = e
                result_container["completed"] = True
        
        invoke_thread = threading.Thread(target=invoke_llm, daemon=True)
        invoke_thread.start()
        invoke_thread.join(timeout=30)
        
        if not result_container["completed"]:
            raise TimeoutError("LLM invoke timed out after 30 seconds")
        
        if result_container["error"]:
            raise result_container["error"]
        
        response = result_container["value"]

        try:
            lesson_data = json.loads(response.content)
            # Return dict instead of LessonResponse to avoid dependency
            return lesson_data
        except json.JSONDecodeError:
            # Fallback if JSON parsing fails
            return self._generate_fallback_lesson(
                topic, learning_objectives, difficulty_level
            )

    def _generate_fallback_lesson(
        self,
        topic: str,
        learning_objectives: List[str],
        difficulty_level: str
    ):
        """Generate fallback lesson when LangChain is unavailable"""
        objectives_str = ', '.join(learning_objectives)
        return {
            "lesson_content": (
                f"Here's a comprehensive lesson on {topic} "
                f"covering {objectives_str}."
            ),
            "key_points": [
                f"Understanding {topic}",
                f"Key concepts in {topic}",
                f"Applications of {topic}"
            ],
            "practice_questions": [
                f"What is {topic}?",
                f"How does {topic} work?",
                f"Give examples of {topic}"
            ],
            "estimated_duration": 45
        }

    def summarize_history(self, history_text: str) -> str:
        """
        Summarize conversation history using gpt-4o-mini.

        Args:
            history_text: Concatenated conversation history text

        Returns:
            str: Summarized history (3-4 sentences) or original text on error
        """
        if not self.openai_client:
            # Fallback: return original text if OpenAI client unavailable
            return history_text

        try:
            import threading
            # Add timeout protection for OpenAI API call (15 seconds)
            result_container = {
                "value": None, "error": None, "completed": False
            }

            def invoke_summarize():
                try:
                    response = self.openai_client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {
                                "role": "system",
                                "content": (
                                    "Summarize the following conversation "
                                    "history into 3-4 concise sentences. "
                                    "Focus on key questions asked and topics "
                                    "discussed."
                                )
                            },
                            {
                                "role": "user",
                                "content": (
                                    f"Conversation history:\n\n{history_text}"
                                )
                            }
                        ],
                        temperature=0,
                        max_tokens=150,
                        timeout=15.0
                    )
                    result_container["value"] = response
                    result_container["completed"] = True
                except Exception as e:
                    result_container["error"] = e
                    result_container["completed"] = True

            summarize_thread = threading.Thread(
                target=invoke_summarize, daemon=True
            )
            summarize_thread.start()
            summarize_thread.join(timeout=15)

            if not result_container["completed"]:
                self.logger.warning(
                    "History summarization timed out, using original text"
                )
                return history_text

            if result_container["error"]:
                self.logger.warning(
                    f"History summarization error: "
                    f"{result_container['error']}, using original text"
                )
                return history_text

            response = result_container["value"]
            return response.choices[0].message.content.strip()
        except Exception as e:
            self.logger.error(f"Error summarizing history: {e}")
            # Fallback: return original text
            return history_text
