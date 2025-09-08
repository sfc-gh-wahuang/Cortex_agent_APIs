WITH ALL_QUESTIONS AS (
    SELECT USER_NAME, LATEST_QUESTION FROM TABLE(
      SNOWFLAKE.LOCAL.CORTEX_ANALYST_REQUESTS(
        'SEMANTIC_VIEW',
        'HOL2_DB.HOL2_SCHEMA.REVENUE'
      )
    )
)
SELECT USER_NAME, AI_AGG(LATEST_QUESTION, 'Summarize the top questions being asked by the user')
FROM ALL_QUESTIONS
GROUP BY USER_NAME
;

WITH ALL_QUESTIONS AS (
    SELECT USER_NAME, LATEST_QUESTION FROM TABLE(
      SNOWFLAKE.LOCAL.CORTEX_ANALYST_REQUESTS(
        'SEMANTIC_VIEW',
        'HOL2_DB.HOL2_SCHEMA.REVENUE'
      )
    )
),
CONCATENATED_QUESTIONS AS (
    SELECT 
        USER_NAME,
        LISTAGG(LATEST_QUESTION, '\n') WITHIN GROUP (ORDER BY LATEST_QUESTION) AS ALL_QUESTIONS
    FROM ALL_QUESTIONS
    GROUP BY USER_NAME
)
SELECT 
    USER_NAME,
    SNOWFLAKE.CORTEX.COMPLETE(
        'CLAUDE-3-5-SONNET',
        CONCAT(
            'Please summarize the top questions being asked by this user. Here are their questions:\n\n',
            ALL_QUESTIONS,
            '\n\nProvide a concise summary of the main themes and topics they are asking about.'
        )
    ) AS QUESTION_SUMMARY
FROM CONCATENATED_QUESTIONS;