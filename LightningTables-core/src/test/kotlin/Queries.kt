package com.coderjoe.lightingtable.core

val queries =
    mapOf(
        "sumCostByUser" to
            """
            SELECT user_id, SUM(cost) as total_cost
            FROM transactions
            GROUP BY user_id
            """.trimIndent(),
    )
