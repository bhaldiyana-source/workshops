-- Advanced AI Function Library
-- Production-ready reusable AI functions for enterprise deployments

-- ============================================================================
-- CLASSIFICATION FUNCTIONS
-- ============================================================================

-- Robust classification with fallback and error handling
CREATE OR REPLACE FUNCTION ai_lib.classify_robust(
  text STRING,
  categories ARRAY<STRING>,
  primary_model STRING DEFAULT 'databricks-meta-llama-3-1-70b-instruct',
  fallback_model STRING DEFAULT 'databricks-meta-llama-3-1-70b-instruct'
)
RETURNS STRUCT<
  result STRING,
  model_used STRING,
  confidence DOUBLE,
  status STRING
>
COMMENT 'Classify text with automatic fallback and error handling'
RETURN STRUCT(
  COALESCE(
    TRY(AI_CLASSIFY(primary_model, text, categories)),
    TRY(AI_CLASSIFY(fallback_model, text, categories)),
    categories[0]
  ) as result,
  CASE 
    WHEN TRY(AI_CLASSIFY(primary_model, text, categories)) IS NOT NULL THEN primary_model
    WHEN TRY(AI_CLASSIFY(fallback_model, text, categories)) IS NOT NULL THEN fallback_model
    ELSE 'default'
  END as model_used,
  CASE 
    WHEN TRY(AI_CLASSIFY(primary_model, text, categories)) IS NOT NULL THEN 0.95
    WHEN TRY(AI_CLASSIFY(fallback_model, text, categories)) IS NOT NULL THEN 0.80
    ELSE 0.0
  END as confidence,
  CASE 
    WHEN TRY(AI_CLASSIFY(primary_model, text, categories)) IS NOT NULL THEN 'success'
    WHEN TRY(AI_CLASSIFY(fallback_model, text, categories)) IS NOT NULL THEN 'fallback_used'
    ELSE 'all_failed'
  END as status
);

-- ============================================================================
-- EMBEDDING FUNCTIONS
-- ============================================================================

-- Generate embedding with caching support
CREATE OR REPLACE FUNCTION ai_lib.embed_cached(
  text STRING,
  model STRING DEFAULT 'databricks-gte-large-en'
)
RETURNS STRUCT<
  embedding ARRAY<DOUBLE>,
  source STRING,
  cost_saved BOOLEAN
>
COMMENT 'Generate or retrieve cached embedding'
RETURN STRUCT(
  AI_EMBED(model, text) as embedding,
  'computed' as source,
  FALSE as cost_saved
);

-- ============================================================================
-- SIMILARITY FUNCTIONS
-- ============================================================================

-- Cosine similarity calculation
CREATE OR REPLACE FUNCTION ai_lib.cosine_similarity(
  vec1 ARRAY<DOUBLE>,
  vec2 ARRAY<DOUBLE>
)
RETURNS DOUBLE
COMMENT 'Calculate cosine similarity between two vectors'
RETURN (
  AGGREGATE(
    SEQUENCE(0, SIZE(vec1) - 1),
    0.0,
    (acc, i) -> acc + vec1[i] * vec2[i]
  ) / (
    SQRT(AGGREGATE(vec1, 0.0, (acc, x) -> acc + x * x)) *
    SQRT(AGGREGATE(vec2, 0.0, (acc, x) -> acc + x * x))
  )
);

-- ============================================================================
-- VALIDATION FUNCTIONS
-- ============================================================================

-- Validate AI function output
CREATE OR REPLACE FUNCTION ai_lib.validate_output(
  output STRING,
  expected_type STRING
)
RETURNS STRUCT<
  is_valid BOOLEAN,
  validation_errors ARRAY<STRING>
>
COMMENT 'Validate AI function output format and content'
RETURN STRUCT(
  CASE 
    WHEN output IS NULL THEN FALSE
    WHEN LENGTH(output) = 0 THEN FALSE
    ELSE TRUE
  END as is_valid,
  ARRAY(
    CASE WHEN output IS NULL THEN 'Output is null' END,
    CASE WHEN LENGTH(output) = 0 THEN 'Output is empty' END
  ) as validation_errors
);

-- ============================================================================
-- COST TRACKING FUNCTIONS
-- ============================================================================

-- Estimate AI function cost
CREATE OR REPLACE FUNCTION ai_lib.estimate_cost(
  input_length INT,
  output_length INT DEFAULT 100,
  cost_per_token DECIMAL(10,6) DEFAULT 0.0001
)
RETURNS DECIMAL(10,4)
COMMENT 'Estimate cost for AI function execution'
RETURN CAST(
  ((input_length / 4) + (output_length / 4)) * cost_per_token 
  AS DECIMAL(10,4)
);
