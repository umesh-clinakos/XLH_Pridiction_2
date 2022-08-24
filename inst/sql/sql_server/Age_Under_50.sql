CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (1421648,40622858,40541688,40642744,40538664,45912492,45912319,45943052,45912714,45935850,1423059,45916550,40389969,4213893,4088386,40321697,4184758,1567852,440979,4228444,37017285,35206739,44833393,44825278,37110336,36713573,36715580,40468608,1408532,4220697,4097961,4250028,4268894,4215791,434894,35206748,40321700,40390437,44825280,36715009,4122928,45929346,4276524,1408407,4257747,1031957,45878117,35918546,439777,40624900,40321260,40309765,36309961,40351339,40321269,43529302,43529996,3045803,4223031,44831063,3476148,37018722,37019193,37017132,45597052,1571925,1571924,44830223,45568004,45539222,45582591,45606669,45606670,45935502,45945112,45929326,45933451,4185887,40582638,4132085,35624756,45600609,4219253,4187768,40354171,4178792,4313413,432875,40389961,4174412,4009306,4244129,35206713,4098747,35206714,4031662,4189321,4330322,444238,35206716,1567842,35206711,4185253,4006467,4042927,40600916,4207240,4158891,4278920,4143629,4323223,4009785,42537687,4260689,4015896,42596489,35206712,4105919,4303199,4104541,4006468,444289,4319914,4201444,4168286,4312008,4194519,4009305,4311676,4312853,40619968,4092893,4144811,432968,432967,40318692,4209094,37016121,4206007,1567857,44831068,45768812,45557089,37398911,37395652,45768813,1033992,40757906,45773534,45757092,434701,44825281,441258,35206749,35206750,4146771,40390439,442923,442922,1010486,35919584,45943051,45948787,45908224,4147365,44836945,40627762,437247,1421660,443961,4307799,4135931,4329173,40498041,40547761,44825602,44833725,44829114,44829115,44822101,44835769,4287402,4243831,4099889,432452,44824612,35210601,4122924,45534311,4071444,45949867,35919506,4022198,4062491,40390443,42596486,4150547,4150154,4343846,4352698,4257721,4341916,45938740,4341917,4340855,4266247,4341918,45907399,4352536,4267475,4343847,4340856,4343848,4341919,4340857,4341920,4341142,4341921,4341922,1414316,45947062,3449686,42872405,35206757,44825282,40356186,40309773,40339213,40390425,40321695,40351340,40389983,44827637,45929127,4280070,44833394,45943042,45916703,45921460,45945080,45929120,45948514,4098023,137829,42542136,40632168,4100999,1435823,44826481,4225810,4184982,4186108,4211348,4187773,37016151,37019055,4101582,4146088,40390416,4101583,35206745,4098027,4100998,1424354,40600609,40617529,40640807,40618302,40523926,40488644,40501065,35206747,44835768,45910628,4230471,40389959,3318952,762942,441269,40455443,40321691,1408504,3439088,4238904,4143167,4209139,4269919,4215784,4039536,36713763,4130680,44833391,37397036,4031204,3122292,40351341,40321264,1408596,3461243,4122926,4177177,37111627,37039419,4005797,37043348,3017022,3024086,3044424,3028448,40796901,37031788,40485052,4005647,36204649,4101455,45946196,4101001,1408569,1408361,40386214,45768941,4318674,1408534,4125630,36907649,43528401,43530302,43530461,43528786,4196495,3083298,45930004,3122343,4160887,40310689,1408505,4101458,40602312,4122927,45929761,45929759,1408490,45948420,45954320,434156,44820048,45934888,4049030,35210602,1408552,35206755,4204062,40390448,36714288,4241982,4286660,4097214,4098007,4175331,1408479,40639002,4178677,1408484,35623048,45926802,44831067,1567855,140681,4098026,4146086,1408447,4008273,4254380,1408414,36714965,43528402,43528409,4227834,434622,4101574,4122079,43528410,4228194,4308062,35206702,1408385,3459912,4122923,4032006,1008287,40769049,915765,43533376,45929038,45932644,35206744,40390058,4146936,35206731,4035974,40390012,1408511,40605005,35206703,4131918,40562656,4131919,4082917,35206733,4300295,42868770,4098748,4342912,4147114,4129707,4250639,40792645,37028429,3003208,3035648,3000573,3001750,40796660,37063114,37023662,3008569,3034505,3033360,40790437,37036192,42599600,4004702,4046137,36210043,36206207,2617148,2617149,2617150,40478891,4188208,45908225,3144333,4148404,45924078,1408551,37109612,36716028,4235788,40623746,45946035,44834247,4178539,4050803,4342928,4257150,4316665,4254751,4254857,4254858,4254750,4353336,4255009,3199897,3199444,3199584,3199326,3199822,3200034,3199392,3200279,3199940,45940127,4148471,43528403,43528411,43022052,1571795,10100,45563133,45596994,45539164,45548779,45577620,45548778,45582523,10101,45567953,45558283,45572834,45592242,45577621,45606603,45577622,10102,45572835,45563134,45592243,45543989,45563135,45606604,45577623,10103,45558284,45606605,45553536,45582524,45553537,45534252,45582525,42596091,45948628,45932214,1567840,4143351,4098009,4098008,4100987,35206705,44831062,42869204,1010932,45909635,4046563,4144077,4079181,4219359,4214023,4336555,2721218,45946181,1553744,4269764,40390010,3440524,45921245,4035316,43528404,43528412,40302037,3090920,3090918,3109443,4077222,3090919,4058711,3090922,40302039,4059188,3090921,45926665,45935848,4159748,4313581,4196102,45878136,1408593,40390395,45954491,45937256,45924057,45915836,45947691,42345729,42336289,42339295,42342499,42345378,42350406,42346566,42336368,42339147,42342354,42345270,42350208,42346606,42336052,42339272,42342466,42345365,42350516,42345651,42346648,42335946,42335999,42339168,42339321,42342278,42342401,42345447,42345291,42350258,42350408,435503,1408498,1435822,4077896,4254392,4281372,4009790,4320480,4005188,4297024,4151502,35624317,4085853,4218100,23988,4101575,4098013,4032352,4297537,4298975,4176884,4098746,4100991,4246105,1408564,45915815,42536531,28396,4098752,35206730,44824097,44836941,3245449,37204551,4115393,3445255,3454743,3467223,3441823,3449084,3470948,3450502,3465440,3428679,3462696,3446790,3466120,3446157,3430376,3162441,3458281,4269075,35206751,42536530,1408430,40480011,40479638,4059187,4323478,4179385,3302339,3305856,40479193,1384907,1385201,1384870,1385634,1385635,43528953,45915689,4031699,36716029,1408366,1408369,46272744,1408584,4218974,45938673,45939371,4098145,35206746,4098627,4173028,1408598,45915623,4298690,4336399,1408517,4231385,4098756,40658868,40662991,45941274,4156842,1408375,45945385,45928030,436659,1567838,4098006,37119138,3471875,4100985,45772084,4058246,35206692,44836940,40628370,433168,44827636,40431416,35206695,44828816,44821803,4120446,36716126,45943385,45943384,4171201,1408402,436820,44831576,4173191,4071073,36714258,45882671,4308125,40389988,42596487,42596488,45932101,4125489,40390949,45914754,4121112,4304950,4098018,1408519,36308494,40390034,40390022,45940706,45954235,45915996,45909488,45927814,45927813,45915995,45944512,45909485,45938630,45940704,435789,4121108,40321267,4184603,4338370,4243950,4168772,4247416,4261354,3469944,4125491,4221567,4291002,4287574,4149183,4008663,4135713,440977,36715492,36715493,36715494,4045142,4099603,4181743,4100962,4147911,4284415,4282785,4147600,4021911,4238731,4338976,4195271,36713571,432588,4096927,1408378,4090690,4034963,4198102,1408518,45944491,4121106,4262948,4172446,1408601,37110070,1408597,1408416,1408602,43528405,43528406,4105643,4223896,1408178,45927722,4098131,1408568,1424884,45915076,4339722,36674478,42514052,45944454,438722,4314111,437834,4100997,44834573,44824098,1408437,1408426,1408429,37110727,45920191,45919528,4114026,4120448,4120450,4116343,4195171,4263315,35910108,45953842,4280354,1435821,35206710,4122080,1408395,1408394,40304547,4064310,500000201,500000202,500000203,500000204,500000205,500001401,3452255,3110340,1408406,45918113,35206738,44832214,1567858,4150157,40321702,35206715,1567854,35206732,35210603,44826482,45557087,44831061,4143352,35206699,35206704,44821804,1567851,35206694,35206707,44835767,35206735,1567841,35206754,4098763,35206756,4101002,44828820,45586119,44825279,44836943,1567856,45557088,4098021,44828819,35206729,4147490,4100986,44824094,4101457,44829907,35206709,4100988,4098012,44833390,4101456,35206700,40389976,45952095,36716259,1408522,915764,4183718,45953690,4306199,438869,44837419,45884619,45924844,44819527,432295,1408373,4079852,45929325,1424151,1424153,1424157,1424155,1424156,45915306,4131915,4130679,4098017,4098753,1408571,35206706,44835765,436083,44783626,4098762,1408432,45915098,45936487,45930986,45908389,40288713,4003185,4028717,1402727,40626069,136949,4028718,1402730,1408182,4264446,4288089,4029670,4002495,35206669,44499399,36402757,36568184,36553657,4190771,4187355,40340542,45557082,45591008,1567832,1408180,40616740,45600597,1408181,40390333,45591007,1408179,35206668,45766614,40367584,4003186,1408175,36715584,4029669,40607435,40390330,1408172,35206667,40550998,4099508,1402728,1402729,35206670,1402731,4150499,4019001,4018378,43529424,43530118,45933723,35206708,44821618,44820498,45954624,4231887,3146858,1408604,4184200,1408565,4131127,37116297,37116298,37116300,37117740,37116301,1408507,4098754,1408516,4101584,35206752,4101000,35206753,4130191,4101578,1408603,1408572,37110923,43528407,43528413,42542233,40321273,40390035,37035795,4100993,45757146,3049047,4131914,4121115,40390037,36209855,37051554,40785343,45925217,432282,44836944,4157495,45942659,45932326,4160238,1408419,43528408,43528414,4307469,40390023,4044728,1408559,4098019,4098760,4155187,4159651,1408431,4030371,45936756,44824095,4101454,42597036,1408384,4125493,1567839,36713572,4147491,35206696,4098740,35206697,1408390,35206701,40389977,45936697,4101573,4098743,42542139,4219853,1408506,45925039,4060269,4102056,40599994,36716460,37204236,36714533,1408415,40390975,40390965,40390004,40390972,40406768,40390956,40390961,40390973,40390979,40390977,40390958,40390337,40385921,3076369,40389980,3059677,40390963,45934609,45948988,45951619,45941444,45910985,45942172,45928916,45933857,45911954,3256631,4120449)

) I
) C UNION ALL 
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (44409671,44409184,44444711,35605687,44438360,42885605,42885606,45269450,35194236,44542503,45692616,45627397,45696189,43049078,43049079,36296893,35614065,42683073,35807057,42901325,43049077,35196088,44141624,40687444,43305595,42683072,36818205,36687832,36687833,36818206,37305760,37305759,43323433,43359539,43332656,40687253,42688531,42689486,40687252,36246801,36246802,44608424,42659383,42659384,36798913,36798914,42659385,36286011,36798915,43025851,43025852,43025853,1559835,36175678,36175676,36178920,36175677,42656012,42656013,36814486,36812958,36504178,37592376,1559834,35605689,35605685,44484722,44467280,42884468,42884324,36788675,36788672,36788671,36788670,42875333,36788669,36788674,36788673,36508461,36507751,36508669,45281903,42901965,45332969,45193521,45299073,42656015,42656016,36788666,36788665,36421480,42875334,36788664,43280270,42683528,44189253,36509967,36509806,37592374,36504798,37592375,44074810,42901326,1184007,1184138,36172621,773719,42506720,1764999,36170464,35510789,44370996,44364589,42808821,45778215,46242144,36788680,35605686,42901327,45633913)

) I
) C
;

with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C


-- End Condition Occurrence Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE
WHERE QE.ordinal = 1
;

--- Inclusion Rule Inserts

select 0 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_0
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Demographic Criteria
SELECT 0 as index_id, e.person_id, e.event_id
FROM #qualified_events E
JOIN @cdm_database_schema.PERSON P ON P.PERSON_ID = E.PERSON_ID
WHERE YEAR(E.start_date) - P.year_of_birth <= 50
GROUP BY e.person_id, e.event_id
-- End Demographic Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 1 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_1
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from #qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM #qualified_events P
JOIN (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) <= 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

SELECT inclusion_rule_id, person_id, event_id
INTO #inclusion_events
FROM (select inclusion_rule_id, person_id, event_id from #Inclusion_0
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_1) I;
TRUNCATE TABLE #Inclusion_0;
DROP TABLE #Inclusion_0;

TRUNCATE TABLE #Inclusion_1;
DROP TABLE #Inclusion_1;


with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups
{2 != 0}?{
  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),2)-1)
}
)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(	
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date 
FROM #final_cohort CO
;

{0 != 0}?{
-- BEGIN: Censored Stats

delete from @results_database_schema.cohort_censor_stats where cohort_definition_id = @target_cohort_id;

-- END: Censored Stats
}
{0 != 0 & 2 != 0}?{

-- Create a temp table of inclusion rule rows for joining in the inclusion rule impact analysis

select cast(rule_sequence as int) as rule_sequence
into #inclusion_rules
from (
  SELECT CAST(0 as int) as rule_sequence UNION ALL SELECT CAST(1 as int) as rule_sequence
) IR;


-- Find the event that is the 'best match' per person.  
-- the 'best match' is defined as the event that satisfies the most inclusion rules.
-- ties are solved by choosing the event that matches the earliest inclusion rule, and then earliest.

select q.person_id, q.event_id
into #best_events
from #qualified_events Q
join (
	SELECT R.person_id, R.event_id, ROW_NUMBER() OVER (PARTITION BY R.person_id ORDER BY R.rule_count DESC,R.min_rule_id ASC, R.start_date ASC) AS rank_value
	FROM (
		SELECT Q.person_id, Q.event_id, COALESCE(COUNT(DISTINCT I.inclusion_rule_id), 0) AS rule_count, COALESCE(MIN(I.inclusion_rule_id), 0) AS min_rule_id, Q.start_date
		FROM #qualified_events Q
		LEFT JOIN #inclusion_events I ON q.person_id = i.person_id AND q.event_id = i.event_id
		GROUP BY Q.person_id, Q.event_id, Q.start_date
	) R
) ranked on Q.person_id = ranked.person_id and Q.event_id = ranked.event_id
WHERE ranked.rank_value = 1
;

-- modes of generation: (the same tables store the results for the different modes, identified by the mode_id column)
-- 0: all events
-- 1: best event


-- BEGIN: Inclusion Impact Analysis - event
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 0 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #qualified_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select @target_cohort_id as cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 0 as mode_id
from #inclusion_rules ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #qualified_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #qualified_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 0 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule'
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 0 as mode_id
FROM
(select count_big(event_id) as total from #qualified_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
  where sr.mode_id = 0 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - event

-- BEGIN: Inclusion Impact Analysis - person
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 1 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #best_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select @target_cohort_id as cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 1 as mode_id
from #inclusion_rules ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #best_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #best_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 1 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule'
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 1 as mode_id
FROM
(select count_big(event_id) as total from #best_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
  where sr.mode_id = 1 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - person

TRUNCATE TABLE #best_events;
DROP TABLE #best_events;

TRUNCATE TABLE #inclusion_rules;
DROP TABLE #inclusion_rules;
}



TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
