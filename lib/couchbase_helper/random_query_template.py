import random


class WhereClause(object):
    def get_where_clause(self, data="default", collection="",
                         num_insert=0, num_update=0, num_delete=0, num_merge=0):
        if data =="employee":
            INSERT_CLAUSE = [
                "join_mo=10:join_mo",
                "job_title='Support':job_title",
                "Working_country='USA':Working_country",
                "citizen_of='INDIA':citizen_of",
                "citizen_of='USA' AND job_title='Sales':citizen_of,job_title",
                "salary > 100000:salary",
                "job_title='Engineer':job_title",
                "join_mo=9:join_mo",
                "job_title='Sales':job_title",
                "'C++' in skills:skills"
                ]
            WHERE_CLAUSE = [
                "join_mo < 3 OR join_mo > 11:period='winter':join_mo",
                "join_day > 25:joined='towards_end':join_day",
                "join_day > 10 AND join_mo > 10:join_month='October':join_day,join_mo",
                "job_title='Engineer' and 'Testing' in skills:QE=True:job_title,skills",
                "job_title='Support':SE=True:job_title",
                "join_yr > 2016 OR job_title='Engineer':mutated=1:join_yr,job_title",
                "join_mo =1:join_month='January':join_mo",
                "job_title='Sales':VISA=['US', 'CANADA']",
                "['Development'] in skills:gaming=True:job_title",
                "citizen_of='INDIA':rating='exceeded_expectation':citizen_of",
                "Working_country='USA':temp_emp=True:Working_country"
            ]
            MERGE_CLAUSE = [
                "target.join_mo = source.month:target.period='winter':UPDATE:join_mo",
                "target.join_day = source.day:target.joined='towards_end':UPDATE:join_day",
                "target.job_title = source.job:target.VISA=['US', 'CANADA']:UPDATE:job_title"
                "target.join_yr = source.year:target.join_yr < 2009:DELETE:join_yr",
                "target.join_yr = source.year and target.join_mo = source.month:target.join_yr < 2009:DELETE:job_title,join_yr",
                "target.job_title = source.job and target.join_day > 25:target.join_yr < 2009:DELETE:job_title,join_day,join_yr",
                "target.join_yr = source.year:{'join_yr': source.year,'join_mo': source.month}:INSERT:join_yr,join_mo",
                "target.job_title = source.job:{'join_yr': source.year,'join_mo': source.month, 'job_title': source.job}:INSERT:job_title,join_yr,join_mo",
                "target.join_yr = source.year and targer.job_title = source.job:{'join_yr': source.year,'join_mo': source.month, 'job_title': source.job}:INSERT:join_mo,join_yr,job_title"]
        else:
            WHERE_CLAUSE = [
                "join_mo < 3 OR join_mo > 11:period='winter'",
                "join_day > 25:joined='towards_end'",
                "join_day > 10 AND join_mo > 10:join_month='October'",
                "job_title='Engineer' and 'Testing' in skills:QE=True",
                "job_title='Support':SE=True",
                "join_yr > 2016 OR job_title='Engineer':mutated=1",
                "join_mo =10:join_month='October'",
                "job_title='Sales':VISA=['US', 'CANADA']",
                "['Development','C++'] in skills:gaming=True",
                "'Development' in skills:isdevloper=True",
                "join_mo=9:join_month='SEPTEMBER'",
                "job_title='Sales':Team='Sales'",
                "job_title='Engineer':Team='Development'"
            ]

        insert_clause = random.sample(INSERT_CLAUSE, num_insert)
        update_clause = random.sample(WHERE_CLAUSE, num_update)
        delete_clause = random.sample(WHERE_CLAUSE, num_delete)
        merge_clause = random.sample(MERGE_CLAUSE, num_merge)
        stmt = self.randomize_query(collection, insert_clause,
                                    update_clause, delete_clause, merge_clause)
        return stmt

    def randomize_query(self, collection,
                        insert_clause=[], update_clause=[], delete_clause=[], merge_clause=[]):
        stmts = []
        for stmt in insert_clause:
            stmts.append("%s:INSERT: %s" % (collection, stmt))
        for stmt in update_clause:
            stmts.append("%s:UPDATE: %s" % (collection, stmt))
        for stmt in delete_clause:
            stmts.append("%s:DELETE: %s" % (collection, stmt))
        for stmt in merge_clause:
            stmts.append("%s:MERGE: %s" % (collection, stmt))
        return stmts
