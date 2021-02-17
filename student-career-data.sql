WITH term_calcs AS (
     SELECT term_id::INTEGER AS base_term,
            term_id::INTEGER + 80 AS term_two,
            term_id::INTEGER + 100 AS year_two,
            term_id::INTEGER + 200 AS year_three,
            term_id::INTEGER + 300 AS year_four,
            term_id::INTEGER + 400 AS year_five,
            term_id::INTEGER + 500 AS year_six,
            term_id::INTEGER + 600 AS year_seven,
            term_id::INTEGER + 700 AS year_eight
       FROM term
      WHERE is_fall_term = TRUE
     ),
pivot_term_calcs AS (
          SELECT a.base_term, 'base_1' AS timeframe, a.base_term AS term_id FROM term_calcs a
UNION ALL SELECT a.base_term, 'term_2', a.term_two   FROM term_calcs a
UNION ALL SELECT a.base_term, 'year_2', a.year_two   FROM term_calcs a
UNION ALL SELECT a.base_term, 'year_3', a.year_three FROM term_calcs a
UNION ALL SELECT a.base_term, 'year_4', a.year_four  FROM term_calcs a
UNION ALL SELECT a.base_term, 'year_5', a.year_five   FROM term_calcs a
UNION ALL SELECT a.base_term, 'year_6', a.year_six   FROM term_calcs a
UNION ALL SELECT a.base_term, 'year_7', a.year_seven FROM term_calcs a
UNION ALL SELECT a.base_term, 'year_8', a.year_eight FROM term_calcs a
    ),
degree_fact AS (
          SELECT a.student_id,
                 a.level_id,
                 a.graduated_term_id,
                 a.degree_id,
                 b.level_id AS degree_level,
                 b.level_desc,
                 COUNT(a.degree_id) AS degree_count,
                 SUM(CASE WHEN b.level_id = 'LA' THEN 1 ELSE 0 END) OVER (PARTITION BY a.student_id, a.level_id
                     ORDER BY a.graduated_term_id ROWS UNBOUNDED PRECEDING) AS deg_certificate,
                 SUM(CASE WHEN b.level_id = 'AS' THEN 1 ELSE 0 END) OVER (PARTITION BY a.student_id, a.level_id
                     ORDER BY a.graduated_term_id ROWS UNBOUNDED PRECEDING) AS deg_associate,
                 SUM(CASE WHEN b.level_id = 'BA' THEN 1 ELSE 0 END) OVER (PARTITION BY a.student_id, a.level_id
                     ORDER BY a.graduated_term_id ROWS UNBOUNDED PRECEDING) AS deg_bachelor,
                 SUM(CASE WHEN b.level_id = 'MA' THEN 1 ELSE 0 END) OVER (PARTITION BY a.student_id, a.level_id
                     ORDER BY a.graduated_term_id ROWS UNBOUNDED PRECEDING) AS deg_master
            FROM degrees_awarded a
      INNER JOIN degree b
              ON a.degree_id = b.degree_id
           WHERE a.is_graduated = TRUE
        GROUP BY a.student_id,
                 a.level_id,
                 a.graduated_term_id,
                 a.degree_id,
                 b.level_id,
                 b.level_desc
        )

SELECT a.student_id,
       a.level_id,
       a.freshman_cohort_code,
       b.base_term,
       b.timeframe,
       b.term_id,
       CASE WHEN c.is_enrolled = TRUE THEN 'Y' ELSE 'N' END AS enrolled,
       CASE WHEN SUM(d.deg_certificate) > 0 THEN 'Y' ELSE 'N' END AS certificate,
       CASE WHEN SUM(d.deg_associate) > 0 THEN 'Y' ELSE 'N' END AS associate,
       CASE WHEN SUM(d.deg_bachelor) > 0 THEN 'Y' ELSE 'N' END AS bachelor,
       a.freshman_cohort_desc,
       a.transfer_cohort_code,
       a.transfer_cohort_desc
     FROM student_term_level a
LEFT JOIN pivot_term_calcs b
       ON a.term_id = b.base_term::TEXT
LEFT JOIN student_term_level c
       ON a.student_id = c.student_id
      AND a.level_id = c.level_id
      AND b.term_id::TEXT = c.term_id
LEFT JOIN degree_fact d
       ON a.student_id = d.student_id
      AND a.level_id = d.level_id
      AND d.graduated_term_id <= b.term_id::TEXT
    WHERE a.freshman_cohort_code = 'FTFB201340'
      AND a.term_id = RIGHT(a.freshman_cohort_code,6)
      AND a.level_id = 'UG'
    GROUP BY a.student_id,
             a.level_id,
             a.freshman_cohort_code,
             b.base_term,
             b.timeframe,
             b.term_id,
             CASE WHEN c.is_enrolled = TRUE THEN 'Y' ELSE 'N' END,
             a.freshman_cohort_desc,
             a.transfer_cohort_code,
             a.transfer_cohort_desc
    ORDER BY 1,5
