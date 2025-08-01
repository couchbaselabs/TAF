import copy
import datetime
import json
import os
import random
import re
import string

from documentgenerator import DocumentGenerator
from data import COUNTRIES, COUNTRY_CODE, FIRST_NAMES, LAST_NAMES

from com.couchbase.client.java.json import JsonObject


class TuqGenerators(object):
    def __init__(self, log, full_set):
        self.log = log
        self.full_set = full_set
        self.query = None
        self.type_args = {}
        self.nests = self._all_nested_objects(full_set[0])
        self.type_args['str'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], unicode)]
        self.type_args['int'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], int)]
        self.type_args['float'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], float)]
        self.type_args['bool'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], bool)]
        self.type_args['list_str'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], list) and isinstance(attr[1][0], unicode)]
        self.type_args['list_int'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], list) and isinstance(attr[1][0], int)]
        self.type_args['list_obj'] = [attr[0] for attr in full_set[0].iteritems()
                            if isinstance(attr[1], list) and isinstance(attr[1][0], dict)]
        self.type_args['obj'] = [attr[0] for attr in full_set[0].iteritems()
                             if isinstance(attr[1], dict)]
        for obj in self.type_args['obj']:
            self.type_args['_obj%s_str' % (self.type_args['obj'].index(obj))] = [attr[0] for attr in full_set[0][obj].iteritems()
                                                                                    if isinstance(attr[1], str)]
            self.type_args['_obj%s_int'% (self.type_args['obj'].index(obj))] = [attr[0] for attr in full_set[0][obj].iteritems()
                                                                                    if isinstance(attr[1], int)]
        for obj in self.type_args['list_obj']:
            self.type_args['_list_obj%s_str' % (self.type_args['list_obj'].index(obj))] = [attr[0] for attr in full_set[0][obj][0].iteritems()
                                                                                    if isinstance(attr[1], str) or isinstance(attr[1], unicode)]
            self.type_args['_list_obj%s_int'% (self.type_args['list_obj'].index(obj))] = [attr[0] for attr in full_set[0][obj][0].iteritems()
                                                                                    if isinstance(attr[1], int)]
        for i in xrange(2, 5):
            self.type_args['nested_%sl' % i] = [attr for attr in self.nests if len(attr.split('.')) == i]
        for i in xrange(2, 5):
            self.type_args['nested_list_%sl' % i] = [attr[0] for attr in self.nests.iteritems() if len(attr[0].split('.')) == i and isinstance(attr[1], list)]
        self._clear_current_query()

    def generate_query(self, template):
        query = template
        for name_type, type_arg in self.type_args.iteritems():
            for attr_type_arg in type_arg:
                query = query.replace('$%s%s' % (name_type, type_arg.index(attr_type_arg)), attr_type_arg)
        for expr in [' where ', ' select ', ' from ', ' order by', ' limit ', 'end',
                     ' offset ', ' count(' , 'group by', 'unnest', 'min', 'satisfies']:
            query = query.replace(expr, expr.upper())
        self.log.info("Generated query to be run: '''%s'''" % query)
        self.query = query
        return query

    def generate_expected_result(self, print_expected_result = True):
        try:
            self._create_alias_map()
            from_clause = self._format_from_clause()
            self.log.info("FROM clause ===== is %s" % from_clause)
            where_clause = self._format_where_clause(from_clause)
            self.log.info("WHERE clause ===== is %s" % where_clause)
            unnest_clause = self._format_unnest_clause(from_clause)
            self.log.info("UNNEST clause ===== is %s" % unnest_clause)
            select_clause = self._format_select_clause(from_clause)
            self.log.info("SELECT clause ===== is %s" % select_clause)
            result = self._filter_full_set(select_clause, where_clause, unnest_clause)
            result = self._order_results(result)
            result = self._limit_and_offset(result)
            if print_expected_result:
                self.log.info("Expected result is %s ..." % str(result[:15]))
            return result
        finally:
            self._clear_current_query()

    def _all_nested_objects(self, d):
        def items():
            for key, value in d.items():
                if isinstance(value, dict):
                    for subkey, subvalue in self._all_nested_objects(value).items():
                        yield key + "." + subkey, subvalue
                else:
                    yield key, value
        return dict(items())

    def _create_alias_map(self):
        query_dict = self.query.split()
        for word in query_dict:
            if word.upper() == 'AS':
                self.aliases[query_dict[query_dict.index(word) + 1]] = query_dict[query_dict.index(word) - 1]

    def _format_where_clause(self, from_clause=None):
        if self.query.find('WHERE') == -1:
            return None
        clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*WHERE', '', self.query))
        clause = re.sub(r'GROUP BY.*', '', clause)
        attributes = self.get_all_attributes()
        conditions = clause.replace('IS NULL', 'is None')
        conditions = conditions.replace('IS NOT NULL', 'is not None')
        satisfy_expr = self.format_satisfy_clause()
        if satisfy_expr:
            conditions = re.sub(r'ANY.*END', '', conditions).strip()
        regex = re.compile("[\w']+\.[\w']+")
        atts = regex.findall(conditions)
        for att in atts:
            parent, child = att.split('.')
            if parent in attributes:
                conditions = conditions.replace(' %s.%s ' % (parent, child),
                                                ' doc["%s"]["%s"] ' % (parent, child))
            else:
                if parent not in self.aliases:
                    conditions = conditions.replace(' %s.%s ' % (parent, child),
                                                ' doc["%s"] ' % (child))
                elif self.aliases[parent] in attributes:
                    conditions = conditions.replace(' %s.%s ' % (parent, child),
                                                    ' doc["%s"]["%s"] ' % (self.aliases[parent], child))
                else:
                    conditions = conditions.replace(' %s.%s ' % (parent, child),
                                                    ' doc["%s"] ' % (child))
        for attr in attributes:
            conditions = conditions.replace(' %s ' % attr, ' doc["%s"] ' % attr)
        if satisfy_expr:
            if conditions:
                for join in ["AND", "OR"]:
                    present = conditions.find(join)
                    if present > -1:
                        conditions = conditions.replace(join, join.lower())
                        if present > 0:
                            conditions += '' + satisfy_expr
                            break
                        else:
                            conditions = satisfy_expr + ' ' + conditions
                            break
            else:
                conditions += '' + satisfy_expr
        if from_clause and from_clause.find('.') != -1:
            sub_attrs = [att for name, group in self.type_args.iteritems()
                         for att in group if att not in attributes]
            for attr in sub_attrs:
                conditions = conditions.replace(' %s ' % attr, ' doc["%s"] ' % attr)
            conditions = conditions.replace('doc[', 'doc["%s"][' % from_clause.split('.')[-1])
        conditions = conditions.replace(' = ', ' == ')
        return conditions

    def _format_from_clause(self):
        clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*FROM', '', self.query)).strip()
        clause = re.sub(r'WHERE.*', '', re.sub(r'GROUP BY.*', '', clause)).strip()
        clause = re.sub(r'SELECT.*', '', clause).strip()
        if len(clause.split()) == 2:
            self.aliases[clause.split()[1]] = clause.split()[0]
        return clause

    def _format_unnest_clause(self, from_clause):
        if from_clause.find('UNNEST') == -1:
            return None
        clause = re.sub(r'.*UNNEST', '', from_clause)
        attr = clause.split()
        if len(attr) == 1:
            clause = 'doc["%s"]' % attr[0]
        elif len(attr) == 2:
            attributes = self.get_all_attributes()
            if attr[0].find('.') != -1:
                splitted = attr[0].split('.')
                if splitted[0] not in attributes:
                    alias = [attr[0].split('.')[1],]
                    clause = 'doc["%s"]' % attr[1]
                    for inner in splitted[2:]:
                        alias.append(inner)
                    self.aliases[attr[1]] = tuple(alias)
                    return clause
                parent, child = attr[0].split('.')
                if parent in attributes:
                    clause = 'doc["%s"]["%s"]' % (parent, child)
                    self.aliases[attr[1]] = (parent, child)
                else:
                    if parent not in self.aliases:
                        clause = 'doc["%s"]' % (child)
                        self.aliases[attr[1]] = child
                    elif self.aliases[parent] in attributes:
                        clause = 'doc["%s"]["%s"]' % (self.aliases[parent], child)
                        self.aliases[attr[1]] = (self.aliases[parent], child)
                    else:
                        clause = 'doc["%s"]' % (child)
                        self.aliases[attr[1]] = child
            else:
                clause = 'doc["%s"]' % attr[0]
                self.aliases[attr[1]] = attr[0]
        elif len(attr) == 3 and ('as' in attr or 'AS' in attr):
            attributes = self.get_all_attributes()
            if attr[0].find('.') != -1:
                parent, child = attr[0].split('.')
                if parent in attributes:
                    clause = 'doc["%s"]["%s"]' % (parent, child)
                    self.aliases[attr[2]] = (parent, child)
                else:
                    if parent not in self.aliases:
                        clause = 'doc["%s"]' % (child)
                        self.aliases[attr[2]] = child
                    elif self.aliases[parent] in attributes:
                        clause = 'doc["%s"]["%s"]' % (self.aliases[parent], child)
                        self.aliases[attr[2]] = (self.aliases[parent], child)
                    else:
                        clause = 'doc["%s"]' % (child)
                        self.aliases[attr[2]] = child
            else:
                clause = 'doc["%s"]' % attr[0]
                self.aliases[attr[2]] = attr[0]
        return clause

    def _format_select_clause(self, from_clause=None):
        select_clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*SELECT', '', self.query)).strip()
        select_clause = re.sub(r'WHERE.*', '', re.sub(r'FROM.*', '', select_clause)).strip()
        select_attrs = select_clause.split(',')
        if from_clause and from_clause.find('UNNEST') != -1:
            from_clause = re.sub(r'UNNEST.*', '', from_clause).strip()
        condition = '{'
        #handle aliases
        for attr_s in select_attrs:
            attr = attr_s.split()
            if re.match(r'COUNT\(.*\)', attr[0]):
                    attr[0] = re.sub(r'\)', '', re.sub(r'.*COUNT\(', '', attr[0])).strip()
                    self.aggr_fns['COUNT'] = {}
                    if attr[0].upper() == 'DISTINCT':
                        attr = attr[1:]
                        self.distinct= True
                    if attr[0].find('.') != -1:
                        parent, child = attr[0].split('.')
                        attr[0] = child
                    if attr[0] in self.aliases:
                        attr[0] = self.aliases[attr[0]]
                    self.aggr_fns['COUNT']['field'] = attr[0]
                    self.aggr_fns['COUNT']['alias'] = ('$1', attr[-1])[len(attr) > 1]
                    if attr[0] == '*':
                        condition += '"%s" : doc,' % attr[-1]
                    continue
            elif re.match(r'MIN\(.*\)', attr[0]):
                    attr[0] = re.sub(r'\)', '', re.sub(r'.*MIN\(', '', attr[0])).strip()
                    self.aggr_fns['MIN'] = {}
                    if attr[0].find('.') != -1:
                        parent, child = attr[0].split('.')
                        attr[0] = child
                    if attr[0] in self.aliases:
                        attr[0] = self.aliases[attr[0]]
                    self.aggr_fns['MIN']['field'] = attr[0]
                    self.aggr_fns['MIN']['alias'] = ('$1', attr[-1])[len(attr) > 1]
                    self.aliases[('$1', attr[-1])[len(attr) > 1]] = attr[0]
                    condition += '"%s": doc["%s"]' % (self.aggr_fns['MIN']['alias'], self.aggr_fns['MIN']['field'])
                    continue
            elif attr[0].upper() == 'DISTINCT':
                attr = attr[1:]
                self.distinct= True
            if attr[0] == '*':
                condition += '"*" : doc,'
            elif len(attr) == 1:
                if attr[0].find('.') != -1:
                    if attr[0].find('[') != -1:
                        condition += '"%s" : doc["%s"]%s,' % (attr[0], attr[0][:attr[0].find('[')], attr[0][attr[0].find('['):])
                    elif attr[0].split('.')[1] == '*':
                        condition = 'doc["%s"]' % (attr[0].split('.')[0])
                        return condition
                    else:
                        if attr[0].split('.')[0] not in self.get_all_attributes() and\
                                        from_clause.find(attr[0].split('.')[0]) != -1:
                            condition += '"%s" : doc["%s"],' % (attr[0].split('.')[1], attr[0].split('.')[1])
                            continue
                        else:
                            condition += '"%s" : {%s : doc["%s"]["%s"]},' % (attr[0].split('.')[0], attr[0].split('.')[1],
                                                                          attr[0].split('.')[0], attr[0].split('.')[1])
                else:
                    if attr[0].find('[') != -1:
                        condition += '"%s" : doc["%s"]%s,' % (attr[0], attr[0][:attr[0].find('[')], attr[0][attr[0].find('['):])
                    else:
                        if attr[0] in self.aliases:
                            value = self.aliases[attr[0]]
                            if len(value) > 1:
                                condition += '"%s" : doc["%s"]' % (attr[0], value[0])
                                for inner in value[1:]:
                                    condition += '["%s"]' % (inner)
                                condition += ','
                        else:
                            condition += '"%s" : doc["%s"],' % (attr[0], attr[0])
            elif len(attr) == 2:
                if attr[0].find('.') != -1:
                    condition += '"%s" : doc["%s"]["%s"],' % (attr[1], attr[0].split('.')[0], attr[0].split('.')[1])
                else:
                    condition += '"%s" : doc["%s"],' % (attr[1], attr[0])
                self.aliases[attr[1]] = attr[0]
            elif len(attr) == 3 and ('as' in attr or 'AS' in attr):
                if attr[0].find('.') != -1:
                    condition += '"%s" : doc["%s"]["%s"],' % (attr[2], attr[0].split('.')[0], attr[0].split('.')[1])
                else:
                    if attr[0].find('[') != -1:
                        condition += '"%s" : doc["%s"]%s,' % (attr[2], attr[0][:attr[0].find('[')], attr[0][attr[0].find('['):])
                    else:
                        condition += '"%s" : doc["%s"],' % (attr[2], attr[0])
        condition += '}'
        if from_clause and from_clause.find('.') != -1:
            condition = condition.replace('doc[', 'doc["%s"][' % from_clause.split('.')[-1])
        return condition

    def _filter_full_set(self, select_clause, where_clause, unnest_clause):
        diff = self._order_clause_greater_than_select(select_clause)
        if diff and not self._is_parent_selected(select_clause, diff) and not 'MIN' in self.query:
            if diff[0].find('][') == -1:
                select_clause = select_clause[:-1] + ','.join(['"%s" : %s' %([at.replace('"','') for at in re.compile('"\w+"').findall(attr)][0],
                                                                            attr) for attr in self._order_clause_greater_than_select(select_clause)]) + '}'
            else:
                for attr in self._order_clause_greater_than_select(select_clause):
                    select_clause = select_clause[:-1]
                    for at in re.compile('"\w+"').findall(attr):
                        if attr.find('][') != -1:
                            attrs_split = [at.replace('"','') for at in re.compile('"\w+"').findall(attr)]
                            select_clause = select_clause + '"%s" : {"%s" : %s},' %(attrs_split[0], attrs_split[1], attr)
                        else:
                            select_clause = select_clause + '"%s" : %s,' %([at.replace('"','') for at in re.compile('"\w+"').findall(attr)][0], attr)
                    select_clause = select_clause + '}'
        if where_clause:
            result = [eval(select_clause) for doc in self.full_set if eval(where_clause)]
        else:
            result = [eval(select_clause) for doc in self.full_set]
        if self.distinct:
            result = [dict(y) for y in set(tuple(x.items()) for x in result)]
        if unnest_clause:
            unnest_attr = unnest_clause[5:-2]
            if unnest_attr in self.aliases:
                def res_generator():
                    for doc in result:
                        doc_temp = copy.deepcopy(doc)
                        del doc_temp[unnest_attr]
                        for item in eval(unnest_clause):
                            doc_to_append = copy.deepcopy(doc_temp)
                            doc_to_append[unnest_attr] = copy.deepcopy(item)
                            yield doc_to_append
                result = list(res_generator())
            else:
                result = [item for doc in result for item in eval(unnest_clause)]
        if self._create_groups()[0]:
            result = self._group_results(result)
        if self.aggr_fns:
            if not self._create_groups()[0] or len(result) == 0:
                for fn_name, params in self.aggr_fns.iteritems():
                    if fn_name == 'COUNT':
                        result = [{params['alias'] : len(result)}]
        return result

    def _order_clause_greater_than_select(self, select_clause):
        order_clause = self._get_order_clause()
        if not order_clause:
            return None
        order_clause = order_clause.replace(',"', '"')
        diff = set(order_clause.split(',')) - set(re.compile('doc\["[\w\']+"\]').findall(select_clause))
        diff = [attr.replace(",",'"') for attr in diff if attr != '']
        for k, v in self.aliases.iteritems():
            if k.endswith(','):
                self.aliases[k[:-1]] = v
                del self.aliases[k]
        if not set(diff) - set(['doc["%s"]' % alias for alias in self.aliases]):
            return None
        else:
            diff = list(set(diff) - set(['doc["%s"]' % alias for alias in self.aliases]))
        if diff:
            self.attr_order_clause_greater_than_select = [re.sub(r'"\].*', '', re.sub(r'doc\["', '', attr)) for attr in diff]
            self.attr_order_clause_greater_than_select = [attr for attr in self.attr_order_clause_greater_than_select if attr]
            return list(diff)
        return None

    def _get_order_clause(self):
        if self.query.find('ORDER BY') == -1:
            return None
        order_clause = re.sub(r'LIMIT.*', '', re.sub(r'.*ORDER BY', '', self.query)).strip()
        order_clause = re.sub(r'OFFSET.*', '', order_clause).strip()
        condition = ""
        order_attrs = order_clause.split(',')
        for attr_s in order_attrs:
            attr = attr_s.split()
            if attr[0] in self.aliases.itervalues():
                    condition += 'doc["%s"],' % (self.get_alias_for(attr[0]))
                    continue
            if attr[0].find('MIN') != -1:
                if 'MIN' not in self.aggr_fns:
                    self.aggr_fns['MIN'] = {}
                    attr[0]= attr[0][4:-1]
                    self.aggr_fns['MIN']['field'] = attr[0]
                    self.aggr_fns['MIN']['alias'] = '$gr1'
                else:
                    if 'alias' in self.aggr_fns['MIN']:
                        condition += 'doc["%s"],' % self.aggr_fns['MIN']['alias']
                        continue
            if attr[0].find('.') != -1:
                attributes = self.get_all_attributes()
                if attr[0].split('.')[0] in self.aliases and (not self.aliases[attr[0].split('.')[0]] in attributes) or\
                   attr[0].split('.')[0] in attributes:
                    condition += 'doc["%s"]["%s"],' % (attr[0].split('.')[0],attr[0].split('.')[1])
                else:
                    if attr[0].split('.')[0].find('[') != -1:
                        ind = attr[0].split('.')[0].index('[')
                        condition += 'doc["%s"]%s["%s"],' % (attr[0].split('.')[0][:ind], attr[0].split('.')[0][ind:],
                                                             attr[0].split('.')[1])
                    else:
                        condition += 'doc["%s"],' % attr[0].split('.')[1]
            else:
                if attr[0].find('[') != -1:
                    ind = attr[0].index('[')
                    condition += 'doc["%s"]%s,' % (attr[0].split('.')[0][:ind], attr[0].split('.')[0][ind:])
                else:
                    condition += 'doc["%s"],' % attr[0]
        self.log.info("ORDER clause ========= is %s" % condition)
        return condition

    def _order_results(self, result):
        order_clause = self._get_order_clause()
        key = None
        reverse = False
        if order_clause:
            all_order_clause = re.sub(r'LIMIT.*', '', re.sub(r'.*ORDER BY', '', self.query)).strip()
            all_order_clause = re.sub(r'OFFSET.*', '', all_order_clause).strip()
            order_attrs = all_order_clause.split(',')
            for attr_s in order_attrs:
                attr = attr_s.split()
                if len(attr) == 2 and attr[1].upper() == 'DESC':
                    reverse = True
            for att_name in re.compile('"[\w\']+"').findall(order_clause):
                if att_name[1:-1] in self.aliases.itervalues():
                    order_clause = order_clause.replace(att_name[1:-1],
                                                        self.get_alias_for(att_name[1:-1]))
                if self.aggr_fns and att_name[1:-1] in [params['field'] for params in self.aggr_fns.itervalues()]:
                    order_clause = order_clause.replace(att_name[1:-1],
                                                        [params['alias'] for params in self.aggr_fns.itervalues()
                                                         if params['field'] == att_name[1:-1]][0])
            if order_clause.find(',"') != -1:
                order_clause = order_clause.replace(',"', '"')
            key = lambda doc: eval(order_clause)
        try:
            result = sorted(result, key=key, reverse=reverse)
        except:
            return result
        if self.attr_order_clause_greater_than_select and not self.parent_selected:
            for doc in result:
                for attr in self.attr_order_clause_greater_than_select:
                    if attr.find('.') != -1:
                        attr = attr.split('.')[0]
                    if attr in doc:
                        del doc[attr]
                    elif '$gr1' in doc:
                        del doc['$gr1']
        return result

    def _limit_and_offset(self, result):
        limit_clause = offset_clause = None
        if self.query.find('LIMIT') != -1:
            limit_clause = re.sub(r'OFFSET.*', '', re.sub(r'.*LIMIT', '', self.query)).strip()
        if self.query.find('OFFSET') != -1:
            offset_clause = re.sub(r'.*OFFSET', '', self.query).strip()
        if offset_clause:
            result = result[int(offset_clause):]
        if limit_clause:
            result = result[:int(limit_clause)]
        return result

    def _create_groups(self):
        if self.query.find('GROUP BY') == -1:
            return 0, None
        group_clause = re.sub(r'ORDER BY.*', '', re.sub(r'.*GROUP BY', '', self.query)).strip()
        if not group_clause:
            return 0, None
        attrs = group_clause.split(',')
        attrs = [attr.strip() for attr in attrs]
        if len(attrs) == 2:
            groups = set([(doc[attrs[0]],doc[attrs[1]])  for doc in self.full_set])
        elif len(attrs) == 1:
            if attrs[0].find('.') != -1:
                if len(attrs[0].split('.')) > 2:
                    groups = set([doc[attrs[0].split('.')[1]][attrs[0].split('.')[2]]
                              for doc in self.full_set])
                else:
                    groups = set([doc[attrs[0].split('.')[0]][attrs[0].split('.')[1]]
                              for doc in self.full_set])
            else:
                groups = set([doc[attrs[0]]  for doc in self.full_set])
        return attrs, groups

    def _group_results(self, result):
        attrs, groups = self._create_groups()
        for fn_name, params in self.aggr_fns.iteritems():
            if fn_name == 'COUNT':
                result = [{attrs[0] : group[0], attrs[1] : group[1],
                                params['alias'] : len([doc for doc in result
                                if doc[attrs[0]]==group[0] and doc[attrs[1]]==group[1]])}
                          for group in groups]
                result = [doc for doc in result if doc[params['alias']] > 0]
            if fn_name == 'MIN':
                if isinstance(list(groups)[0], tuple):
                    result = [{attrs[0] : group[0], attrs[1] : group[1],
                                    params['alias'] : min([doc[params['field']] for doc in result
                                    if doc[attrs[0]]==group[0] and doc[attrs[1]]==group[1]])}
                              for group in groups]
                else:
                    if attrs[0] in self.aliases.itervalues():
                        attrs[0] = self.get_alias_for(attrs[0]).replace(',', '')
                    result = [{attrs[0] : group,
                                params['alias'] : min([doc[params['alias']] for doc in result
                                if doc[attrs[0]]==group])}
                          for group in groups]
        else:
            result = [dict(y) for y in set(tuple(x.items()) for x in result)]
        return result

    def get_alias_for(self, value_search):
        for key, value in self.aliases.iteritems():
            if value == value_search:
                return key
        return ''

    def get_all_attributes(self):
        return [att for name, group in self.type_args.iteritems()
                for att in group if not name.startswith('_')]

    def _is_parent_selected(self, clause, diff):
        self.parent_selected = len([select_el for select_el in re.compile('doc\["[\w\']+"\]').findall(clause)
                for diff_el in diff if diff_el.find(select_el) != -1]) > 0
        return self.parent_selected

    def format_satisfy_clause(self):
        if self.query.find('ANY') == -1 and self.query.find('EVERY') == -1:
            return ''
        satisfy_clause = re.sub(r'.*ANY', '', re.sub(r'END.*', '', self.query)).strip()
        satisfy_clause = re.sub(r'.*ALL', '', re.sub(r'.*EVERY', '', satisfy_clause)).strip()
        if not satisfy_clause:
            return ''
        main_attr = re.sub(r'SATISFIES.*', '', re.sub(r'.*IN', '', satisfy_clause)).strip()
        attributes = self.get_all_attributes()
        if main_attr in attributes:
            main_attr = 'doc["%s"]' % (main_attr)
        else:
            if main_attr.find('.') != -1:
                parent, child = main_attr.split('.')
                if parent in self.aliases and self.aliases[parent] in attributes:
                    main_attr = 'doc["%s"]["%s"]' % (self.aliases[parent], child)
                else:
                    main_attr = 'doc["%s"]' % (child)
        var = "att"
        if self.query.find('ANY') != -1:
            var = re.sub(r'.*ANY', '', re.sub(r'IN.*', '', self.query)).strip()
            result_clause = 'len([{0} for {1} in {2} if '.format(var, var, main_attr)
        satisfy_expr = re.sub(r'.*SATISFIES', '', re.sub(r'END.*', '', satisfy_clause)).strip()
        for expr in satisfy_expr.split():
            if expr.find('.') != -1:
                result_clause += ' {0}["{1}"] '.format(var, expr.split('.')[1])
            elif expr.find('=') != -1:
                result_clause += ' == '
            elif expr.upper() in ['AND', 'OR', 'NOT']:
                result_clause += expr.lower()
            else:
                result_clause += ' %s ' % expr
        result_clause += ']) > 0'
        return result_clause

    def _clear_current_query(self):
        self.distinct = False
        self.aggr_fns = {}
        self.aliases = {}
        self.attr_order_clause_greater_than_select = []
        self.parent_selected = False


class JsonGenerator:

    def generate_docs_employee(self, key_prefix, docs_per_day=1, start=0, isShuffle=False):
        types = ['Engineer', 'Sales', 'Support']
        skills = ['Python', 'Java', 'C++', 'Testing', 'Development']
        join_yr = [2010, 2011,2012,2013,2014,2015,2016]
        join_mo = xrange(1, 12 + 1)
        join_day = xrange(1, 28 + 1)
        templates = []
        for i in xrange(start, docs_per_day):
            random.seed(i)
            month = random.choice(join_mo)
            prefix = "_employee"+str(i)
            name = "employee-%s" % (str(i))
            email = ["%s-mail@couchbase.com" % (str(i))]
            vms = [{"RAM": month, "os": "ubuntu",
                    "name": "vm_%s" % month, "memory": month},
                   {"RAM": month, "os": "windows",
                    "name": "vm_%s"% (month + 1), "memory": month}
                 ]
            template = JsonObject.create()
            template.put("name", name)
            template.put("join_yr", random.choice(join_yr))
            template.put("join_mo" , month)
            template.put("join_day" , random.choice(join_day))
            template.put("email" , email)
            template.put("job_title" , random.choice(types))
            template.put("test_rate" , xrange(1, 10))
            template.put("skills" , random.sample(skills, 2))
            template.put("vms" , [vms])
            templates.append(template)
        gen_load = DocumentGenerator(key_prefix, templates,
                                start=start, end=docs_per_day)
        return gen_load

    def generate_docs_employee_more_field_types(self, key_prefix, docs_per_day=1, start=0):
        types = ['Engineer', 'Sales', 'Support', "Pre-Sales", "Info-Tech",
                 "Accounts", "Dev-ops", "Cloud", "Training", ""]
        skills = ['Python', 'Java', 'C++', 'Testing', 'Development']
        Firstname = ["Bryn", "Kallie", "Trista", "Riona", "Salina", "Ebony", "Killian",
                      "Sirena", "Treva", "Ambika", "Lysha", "Hedda", "Lilith", "Bryn",
                      "Kacila", "Quella", "Deirdre", "Adrianne", "Drucilla", "Hedda", "Mia",
                      "Alvita", "Fantine", "Maura", "Basha", "Jerica", "Severin", "Lysha",
                      "Adena", "Fuscienne", "Adrianne", "Salina", "Cassandra", "Winta"]
        Second_name = ["Russell", "Green", "Thomas", "Julian", "Baker", "cox", "copper",
                       "Wilson", "Clark", "Scott", "Wright", "Howard Jr.", "Juan Jose",
                       "Young", "Evans", "Sebastian", "Phillips", "Carter", "White",
                       "Davis", "Palmer", "Morgan", "Damian", "Brown", "Reed", "Bailey",
                       "Jones", "Cook", "Hall", "Wood", "Nicolas", "Moore", "Lee",
                       "Jackson", "Tomas", "Wright"]
        countries = ["India", "USA", "CANADA", "AUSTRALIA"]
        languanges = ["Sinhalese", "German", "Italian", "English", "Quechua",
                            "Portuguese", "Thai", "Hindi", "Africans", "Urdu", "Malay",
                            "Spanish", "French", "Nepalese", "Dutch", "Vietnamese", "Arabic",
                            "Japanese"]
        join_yr = [2010, 2011,2012,2013,2014,2015,2016]
        join_mo = xrange(1, 12 + 1)
        join_day = xrange(1, 28 + 1)
        is_manager = [True,False]
        salary = xrange(10000, 200000)
        emp_id = xrange(0, 10000)
        count = 1
        templates = []
#             languanges_known = {}
#             languanges_known["first"] = random.choice(languanges)
#             languanges_known["second"] = random.choice(languanges)
#             languanges_known["third"] = random.choice(languanges)
#             if is_manager:
#                 manages = {}
#                 manages["team_size"] = random.choice(range(4, 8))
#                 manages["reports"] = []
#                 for _ in range(manages["team_size"]):
#                     manages["reports"].append(random.choice(Firstname) + " "

#             template.put("name", name)
#             template.put("email" , email)
#             template.put("languanges_known",languanges_known)
#             if is_manager:
#                 template.put("manages", manages)
        template = JsonObject.create()
        template.put("name", None)
        template.put("join_yr", None)
        template.put("join_mo" , None)
        template.put("join_day" , None)
#         template.put("email" , email)
        template.put("emp_id", None)
        template.put("job_title" , None)
        template.put("skills" ,None)
        template.put("is_manager", None)
        template.put("salary", None)
        template.put("citizen_of", None)
        template.put("Working_country", None)
        template.put("languanges_known", None)
        template.put("key", None)
#         if is_manager:
#             template.put("manages", manages)
        templates.append(template)
        gen_load = DocumentGenerator(key_prefix,
                                               template,
                                               name=lambda: random.choice(Firstname) + " " + random.choice(Second_name),
                                               join_yr=join_yr,
                                               join_mo=join_mo, emp_id=emp_id,
                                               job_title=types, skills=skills,
                                               is_manager=is_manager,
                                               salary=salary, citizen_of=countries,
                                               Working_country=countries,
                                               languanges_known=languanges,
                                               join_day=join_day, randomize=True,
                                               deep_copy=True,
                                               start=start, end=docs_per_day)
        return gen_load

    def generate_docs_employee_array(self, key_prefix, docs_per_day=1, start=0):
        generators = []
        #simple array
        department = ['Developer', 'Support','HR','Tester','Manager']
        sport = ['Badminton','Cricket','Football','Basketball','American Football','ski']
        dance = ['classical','bollywood','salsa','hip hop','contemporary','bhangra']
        join_yr = [2010, 2011,2012,2013,2014,2015,2016]
        join_mo = xrange(1, 12 + 1)
        join_day = xrange(1, 28 + 1)
        engineer = ["Query","Search","Indexing","Storage","Android","IOS"]
        marketing = ["East","West","North","South","International"]
        cities = ['Mumbai','Delhi','New York','San Francisco']
        streets = ['21st street','12th street','18th street']
        countries = ['USA','INDIA','EUROPE']
        template = '{{ "name":{0}  , "department": "{1}" , "join_yr":{2},'
        template += ' "email":"{3}", "hobbies": {{ "hobby" : {4} }},'
        template += ' "tasks":  {5},  '
        template += '"VMs": {6} , '
        template += '"address" : {7} }}'
        count = 1
        templates = []
        for i in xrange(start, docs_per_day):
            random.seed(count)
            month = random.choice(join_mo)
            prefix = "employee" + str(i)
            # array of  single objects
            name = [{"FirstName": "employeefirstname-%s" % (str(i))},
                    {"MiddleName": "employeemiddlename-%s" % (str(i))},
                    {"LastName": "employeelastname-%s" % (str(i))}]

            # array inside array inside object
            sportValue = random.sample(sport, 3)
            danceValue = random.sample(dance, 3)
            hobbies = [{"sports": sportValue}, {"dance": danceValue},"art"]
            email = ["%s-mail@couchbase.com" % (str(i))]
            joining = random.sample(join_yr,3)
            # array inside array
            enggValue = random.sample(engineer, 2)
            marketingValue = [{"region1" :random.choice(marketing),"region2" :random.choice(marketing)},{"region2" :random.choice(marketing)}]
            taskValue = [{"Developer": enggValue,"Marketing": marketingValue},"Sales","QA"]
            # array of multiple objects
            vms = [{"RAM": month, "os": "ubuntu",
                    "name": "vm_%s" % month, "memory": month},
                   {"RAM": month, "os": "windows",
                    "name": "vm_%s" % (month + 1), "memory": month},
                   {"RAM": month, "os": "centos", "name": "vm_%s" % (month + 2), "memory": month},
                   {"RAM": month, "os": "macos", "name": "vm_%s" % (month + 3), "memory": month}
                   ]
            addressvalue = [[ {"city": random.choice(cities)},{"street":random.choice(streets)}],[{"apartment":123,"country":random.choice(countries)}]]
            count += 1
            template = JsonObject.create()
            template.put("fullname", [name])
            template.put("department", random.choice(department))
            template.put("join_yr", [[ y for y in joining]])
            template.put("email", email)
            template.put("hobbies", random.choice(hobbies))
            template.put("vms", [vms])
            template.put("address", [addressvalue])
            template.put("task", taskValue)
            templates.append(template)
        gen_docs = DocumentGenerator(key_prefix, templates,
                                        start=start, end=docs_per_day)

        return gen_docs

    def generate_docs_sabre(self, key_prefix, docs_per_day=1, start=0, isShuffle=False, years=2, indexes=[1,4,8]):
        all_airports = ["ABR", "ABI", "ATL","BOS", "BUR", "CHI", "MDW", "DAL", "SFO", "SAN", "SJC", "LGA", "JFK", "MSP",
                        "MSQ", "MIA", "LON", "DUB"]
        dests = [all_airports[i] for i in indexes]
        join_yr = self._shuffle(xrange(2010, 2010 + years), isShuffle)
        join_mo = self._shuffle(xrange(1, 12 + 1),isShuffle)
        join_day = self._shuffle(xrange(1, 28 + 1),isShuffle)
        count = 1
        templates = []
        for i in xrange(start, docs_per_day):
            random.seed(count)
            dest = random.choice(all_airports)
            year = random.choice(join_yr)
            month = random.choice(join_mo)
            day = random.choice(join_day)
            count +=1
            prefix = '%s_%s-%s-%s' % (dest, year, month, day)
            amount = [float("%s.%s" % (month, month))]
            currency = [("USD", "EUR")[month in [1,3,5]]]
            decimal_tax = [1,2]
            amount_tax = [day]
            currency_tax = currency
            taxes = [{"DecimalPlaces": 2, "Amount": float(amount_tax[0])/3,
                      "TaxCode": "US1", "CurrencyCode": currency},
                     {"DecimalPlaces": 2, "Amount": float(amount_tax[0])/4,
                      "TaxCode": "US2", "CurrencyCode": currency},
                     {"DecimalPlaces": 2, "Amount": amount_tax[0] - float(amount_tax[0])/4-\
                      float(amount_tax[0])/3,
                      "TaxCode": "US2", "CurrencyCode": currency}]
            fare_basis = [{"content": "XA21A0NY", "DepartureAirportCode": dest,
                           "BookingCode": "X", "ArrivalAirportCode": "MSP"},
                          {"content": "XA21A0NY", "DepartureAirportCode": "MSP",
                           "AvailabilityBreak": True, "BookingCode": "X",
                           "ArrivalAirportCode": "BOS"}]
            pass_amount = [day]
            ticket_type = [("eTicket", "testType")[month in [1,3,5]]]
            sequence = [year]
            direction = [("oneWay", "return")[month in [2,6,10]]]
            itinerary = {"OriginDestinationOptions":
                         {"OriginDestinationOption": [
                           {"FlightSegment": [
                             {"TPA_Extensions":
                               {"eTicket": {"Ind": True}},
                               "MarketingAirline": {"Code": dest},
                               "StopQuantity": month,
                               "DepartureTimeZone": {"GMTOffset": -7},
                               "OperatingAirline": {"Code": "DL",
                                                    "FlightNumber": year + month},
                               "DepartureAirport": {"LocationCode": "SFO"},
                               "ArrivalTimeZone": {"GMTOffset": -5},
                               "ResBookDesigCode": "X",
                               "FlightNumber": year + day,
                               "ArrivalDateTime": "2014-07-12T06:07:00",
                               "ElapsedTime": 212,
                               "Equipment": {"AirEquipType": 763},
                               "DepartureDateTime": "2014-07-12T00:35:00",
                               "MarriageGrp": "O",
                               "ArrivalAirport": {"LocationCode": random.sample(all_airports, 1)}},
                            {"TPA_Extensions":
                               {"eTicket": {"Ind": False}},
                               "MarketingAirline": {"Code": dest},
                               "StopQuantity": month,
                               "DepartureTimeZone": {"GMTOffset": -7},
                               "OperatingAirline": {"Code": "DL",
                                                    "FlightNumber": year + month + 1},
                               "DepartureAirport": {"LocationCode": random.sample(all_airports, 1)},
                               "ArrivalTimeZone": {"GMTOffset": -3},
                               "ResBookDesigCode": "X",
                               "FlightNumber": year + day,
                               "ArrivalDateTime": "2014-07-12T06:07:00",
                               "ElapsedTime": 212,
                               "Equipment": {"AirEquipType": 764},
                               "DepartureDateTime": "2014-07-12T00:35:00",
                               "MarriageGrp": "1",
                               "ArrivalAirport": {"LocationCode": random.sample(all_airports, 1)}}],
                        "ElapsedTime": 619},
                       {"FlightSegment": [
                             {"TPA_Extensions":
                               {"eTicket": {"Ind": True}},
                               "MarketingAirline": {"Code": dest},
                               "StopQuantity": month,
                               "DepartureTimeZone": {"GMTOffset": -7},
                               "OperatingAirline": {"Code": "DL",
                                                    "FlightNumber": year + month},
                               "DepartureAirport": {"LocationCode": random.sample(all_airports, 1)},
                               "ArrivalTimeZone": {"GMTOffset": -5},
                               "ResBookDesigCode": "X",
                               "FlightNumber": year + day,
                               "ArrivalDateTime": "2014-07-12T06:07:00",
                               "ElapsedTime": 212,
                               "Equipment": {"AirEquipType": 763},
                               "DepartureDateTime": "2014-07-12T00:35:00",
                               "MarriageGrp": "O",
                               "ArrivalAirport": {"LocationCode": random.sample(all_airports, 1)}},
                            {"TPA_Extensions":
                               {"eTicket": {"Ind": False}},
                               "MarketingAirline": {"Code": dest},
                               "StopQuantity": month,
                               "DepartureTimeZone": {"GMTOffset": -7},
                               "OperatingAirline": {"Code": "DL",
                                                    "FlightNumber": year + month + 1},
                               "DepartureAirport": {"LocationCode": random.sample(all_airports, 1)},
                               "ArrivalTimeZone": {"GMTOffset": -3},
                               "ResBookDesigCode": "X",
                               "FlightNumber": year + day,
                               "ArrivalDateTime": "2014-07-12T06:07:00",
                               "ElapsedTime": 212,
                               "Equipment": {"AirEquipType": 764},
                               "DepartureDateTime": "2014-07-12T00:35:00",
                               "MarriageGrp": "1",
                               "ArrivalAirport": {"LocationCode": random.sample(all_airports, 1)}}]}]},
                         "DirectionInd": "Return"}
            TotalTax = {"DecimalPlaces" : decimal_tax, "Amount" : amount_tax,\
            "CurrencyCode" : currency_tax}
            template = JsonObject.create()
            template.put("Amount", amount)
            template.put("CurrencyCode", currency)
            template.put("TotalTax", TotalTax)
            template.put("Tax", [taxes])
            template.put( "FareBasisCode", [fare_basis])
            template.put("PassengerTypeQuantity", pass_amount)
            template.put("TicketType", ticket_type)
            template.put("SequenceNumber", sequence)
            template.put("DirectionInd", direction)
            template.put("Itinerary", [itinerary])
            template.put("Destination", [dest])
            template.put("join_yr", [year])
            template.put("join_mo", [month])
            template.put("join_day", [day])
            template.put("Codes",[[dest, dest]])
            templates.append(template)
            count += 1
        gen_load = DocumentGenerator(key_prefix, templates,
                                                start=start, end=docs_per_day)
        return gen_load

    def generate_docs_sales(self, key_prefix = "sales_dataset", test_data_type = True, start=0, docs_per_day=None, isShuffle = False):
        end = docs_per_day
        join_yr = [2010, 2011,2012,2013,2014,2015,2016]
        join_mo = xrange(1, 12 + 1)
        join_day = xrange(1, 28 + 1)
        is_support = ['true', 'false']
        is_priority = ['true', 'false']
        count = 1
        templates = []
        sales = [200000, 400000, 600000, 800000]
        rate = [x * 0.1 for x in xrange(0, 10)]
        for i in xrange(start, docs_per_day):
            contact = "contact_"+ str(random.random()*10000000)
            name ="name_"+ str(random.random()*100000)
            year = random.choice(join_yr)
            month = random.choice(join_mo)
            day = random.choice(join_day)
            random.seed(count)
            count +=1
            prefix = "prefix_"+str(i)
            delivery = str(datetime.date(year, month, day))
            template = JsonObject.create()
            template.put("join_yr" , [year])
            template.put("join_mo" , [month])
            template.put("join_day" , [day])
            template.put("sales" , random.choice(sales))
            template.put("delivery_date" , [delivery])
            template.put("is_support_included" , random.choice(is_support))
            template.put("is_high_priority_client" , random.choice(is_priority))
            template.put("client_contact" , [contact])
            template.put("client_name" , [name])
            template.put("client_reclaims_rate" , random.choice(rate))
            templates.append(template)
        gen_load = DocumentGenerator(key_prefix,
                                                  templates,
                                                  start=start, end=end)
        return gen_load

    def generate_docs_bigdata(self, key_prefix="big_dataset", value_size=1024,
                              start=0, docs_per_day=1, end=None):
        if end is None:
            end = docs_per_day
        age = xrange(start, end)
        name = ['a' * value_size]
        template = JsonObject.create()
        template.put("age", age)
        template.put("name", name)

        gen_load = DocumentGenerator(key_prefix, template,
                                     start=start, end=end,
                                     randomize=True,
                                     age=age)
        return gen_load

    def generate_docs_simple(self, key_prefix="simple_dataset", start=0,
                             docs_per_day=1000, isShuffle=False):
        end = docs_per_day
        age = self._shuffle(range(start, end), isShuffle)
        name = [key_prefix + '-' + str(i)
                for i in self._shuffle(xrange(start, end), isShuffle)]
        template = JsonObject.create()
        template.put("age", age)
        template.put("name", name)
        gen_load = DocumentGenerator(key_prefix, template,
                                     start=start, end=end,
                                     randomize=True,
                                     name=name,
                                     age=age)
        return gen_load

    def generate_docs_array(self, key_prefix="array_dataset", start=0,
                            docs_per_day=1):
        COUNTRIES = ["India", "US", "UK", "Japan", "France", "Germany", "China", "Korea", "Canada", "Cuba",
             "West Indies", "Australia", "New Zealand", "Nepal", "Sri Lanka", "Pakistan", "Mexico",
             "belgium", "Netherlands", "Brazil", "Costa Rica", "Cambodia", "Fiji", "Finland", "haiti",
             "Hong Kong", "Iceland", "Iran", "Iraq", "Italy", "Greece", "Jamaica", "Kenya", "Kuwait", "Macau",
             "Spain","Morocco", "Maldives", "Norway"]

        COUNTRY_CODE = ["Ind123", "US123", "UK123", "Jap123", "Fra123", "Ger123", "Chi123", "Kor123", "Can123",
                "Cub123", "Wes123", "Aus123", "New123", "Nep123", "Sri123", "Pak123", "Mex123", "bel123",
                "Net123", "Bra123", "Cos123", "Cam123", "Fij123", "Fin123", "hai123", "Hon123", "Ice123",
                "Ira123", "Ira123", "Ita123", "Gre123", "Jam123", "Ken123", "Kuw123", "Mac123", "Spa123",
                "Mor123", "Mal123", "Nor123"]
        end = docs_per_day
        templates = []
        for i in xrange(start, end):
            countries = []
            codes = []
            random.seed(i)
            name = ["Passenger-{0}".format(i)]
            email = ["passenger_{0}@abc.com".format(i)]
            start_pnt = random.randint(0, len(COUNTRIES)-2)
            end_pnt = random.randint(start_pnt, len(COUNTRIES)-1)
            cnt = COUNTRIES[start_pnt:end_pnt]
            countries.append(cnt)
            cde = COUNTRY_CODE[start_pnt:end_pnt]
            codes.append(cde)
            prefix = "{0}-{1}".format(key_prefix,i)
            template = JsonObject.create()
            template.put("email", email)
            template.put("name", name)
            template.put("countries", countries)
            template.put("code", codes)
            templates.append(template)
        gen_load = DocumentGenerator(prefix, templates,
                                                 start=start, end=end)
        return gen_load

    def random_date(self):
        earliest = datetime.date(1910,1,1)
        latest  = datetime.date(2018,1,1)
        delta = latest - earliest
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return earliest + datetime.timedelta(seconds = random_second)

    def generate_earthquake_doc(self, key_prefix="array_dataset", start=0,
                            end=1):
        Src = ['ac', 'ci', 'ak', 'pr', 'us', 'uw']
        Region = ["Central California", "Lassen Peak area, California", "Southern California",
                   "Rat Islands, Aleutian Islands, Alaska","offshore Oregon", "Andreanof Islands",
                    "Aleutian Islands, Alaska", "Southern Alaska", "Virgin Islands region",
                    "Baja California, Mexico", "Alaska Peninsula", "Central Alaska", "Northern California",
                    "San Francisco Bay area, California","Fox Islands, Aleutian Islands, Alaska",
                    "Central California", "Unimak Island region, Alaska","Oaxaca, Mexico", "Kenai Peninsula, Alaska",
                    "Washington", "Guatemala", "Anegada, British Virgin Islands","Dominican Republic region",
                    "Nevada", "Island of Hawaii, Hawaii", "Kodiak Island region, Alaska", "western Honshu, Japan",
                    "Philippine Islands region", "Hindu Kush region, Afghanistan", "Kodiak Island region, Alaska",
                    "Long Valley area, California", "Wasatch Front Urban Corridor, Utah", "San Pedro Channel, California",
                    "Greater Los Angeles area, California", "Tarapaca, Chile","Arkansas", "Reykjanes Ridge",
                    "Alaska Peninsula", "Gulf of California", "Turkmenistan", "Oregon", "Bismarck Sea",
                    "Myanmar", "Puerto Rico region","Banda Sea", "Tonga", "offshore Oregon", "Kodiak Island region, Alaska",
                    "Mindanao, Philippines","Yellowstone National Park, Wyoming", "Greece", "south of Java, Indonesia",
                    "Kenai Peninsula, Alaska", "eastern Turkey","Baja California, Mexico", "Kepulauan Tanimbar, Indonesia",
                    "Nevada", "Utah", "Ontario-Quebec border region, Canada", "Timor region", "Pagan region, Northern Mariana Islands",
                    "Mendoza, Argentina", "Antofagasta, Chile", "New Britain region, Papua New Guinea", "Nevada", "Missouri",
                    "Solomon Islands", "offshore El Salvador", "eastern Mediterranean Sea", "Costa Rica", "Tanzania",
                    "Seattle-Tacoma urban area, Washington","Mona Passage, Puerto Rico",
                    "Mindanao, Philippines", "Philippine Islands region", "Halmahera, Indonesia",
                    "Newberry Caldera area, Oregon", "south of Africa", "south of the Mariana Islands", "Ascension Island region"]
        Magnitude = [1.1, 1.9, 2.3, 4, 6, 7.2, 5.3, 3.7, 9.2, 7.1, 6.3, 6.7, 6.5]
        Depth = xrange(10, 40)
        Version = xrange(3)
        NST = xrange(1, 10)
        templates = []
        for i in xrange(start, end):
            random.seed(i)
            prefix = "{0}-{1}".format(key_prefix,i)
            template = JsonObject.create()
            template.put("src", random.choice(Src))
            template.put("Region", random.choice(Region))
            template.put("Magnitude", random.choice(Magnitude))
            template.put("Depth", random.choice(Depth))
            template.put("Version", random.choice(Version))
            template.put("NST", random.choice(NST))
            random_date = self.random_date()
            template.put("Date", random_date)
            templates.append(template)
        gen_load = DocumentGenerator(prefix, templates,
                                                 start=start, end=end)
        return gen_load

    def generate_all_type_documents_for_gsi(self, start=0, docs_per_day=10):
        """
        Document fields:
        name: String
        age: Number
        email: Alphanumeric + Special Character
        premium_customer: Boolean or <NULL>
        Address: Object
                {Line 1: Alphanumeric + Special Character
                Line 2: Alphanumeric + Special Character or <NULL>
                City: String
                Country: String
                postal_code: Number
                }
        travel_history: Array of string - Duplicate elements ["India", "US", "UK", "India"]
        travel_history_code: Array of alphanumerics - Duplicate elements
        booking_history: Array of objects
                        {source:
                         destination:
                          }
        credit_cards: Array of numbers
        secret_combination: Array of mixed data types
        countries_visited: Array of strings - non-duplicate elements

        :param start:
        :param docs_per_day:
        :param isShuffle:
        :return:
        """
        generators = []
        bool_vals = [True, False]
        templates = []
        template = r'{{ "name":"{0}", "email":"{1}", "age":{2}, "premium_customer":{3}, ' \
                   '"address":{4}, "travel_history":{5}, "travel_history_code":{6}, "travel_details":{7},' \
                   '"booking":{8}, "credit_cards":{9}, "secret_combination":{10}, "countries_visited":{11}, ' \
                   '"question_values":{12}}}'
        for i in xrange(docs_per_day):
            name = random.choice(FIRST_NAMES)
            age = random.randint(25, 70)
            last_name = random.choice(LAST_NAMES)
            dob = "{0}-{1}-{2}".format(random.randint(1970, 1999),
                                       random.randint(1, 28), random.randint(1, 12))
            email = "{0}.{1}.{2}@abc.com".format(name, last_name, dob.split("-")[1])
            premium_customer = random.choice(bool_vals)
            address = {}
            address["line_1"] = "Street No. {0}".format(random.randint(100, 200))
            address["line_2"] = "null"
            if not random.choice(bool_vals):
                address["address2"] = "Building {0}".format(random.randint(1, 6))
            address["city"] = "Bangalore"
            address["contact"] = "{0} {1}".format(name, last_name)
            address["country"] = "India"
            address["postal_code"] = "{0}".format(random.randint(560071, 560090))
            credit_cards = [random.randint(-1000000, 9999999) for i in xrange(random.randint(3, 7))]
            secret_combo = [''.join(random.choice(string.lowercase) for i in xrange(7)),
                            random.randint(1000000, 9999999)]
            travel_history = [random.choice(COUNTRIES[:9]) for i in xrange(1, 11)]
            travel_history_code = [COUNTRY_CODE[COUNTRIES.index(i)] for i in travel_history]
            travel_details = [{"country": travel_history[i], "code": travel_history_code[i]}
                              for i in xrange(len(travel_history))]
            countries_visited = list(set(travel_history))
            booking = {"source": random.choice(COUNTRIES), "destination": random.choice(COUNTRIES)}
            confirm_question_values = [random.choice(bool_vals) for i in xrange(5)]
            prefix = "airline_record_" + str(random.random()*100000)
            template = JsonObject.create()
            template.put("name", [name])
            template.put("email", email)
            template.put("age", age)
            template.put("premium_customer", premium_customer)
            template.put("travel_history", travel_history)
            template.put("travel_history_code", travel_history_code)
            template.put("travel_details", travel_details)
            template.put("booking", booking)
            template.put("credit_cards", credit_cards)
            template.put("secret_combo", secret_combo)
            template.put("countries_visited", countries_visited)
            template.put("confirm_question_values", confirm_question_values)
            templates.append(template)
        gen_load = DocumentGenerator(prefix, template, start=start, end=docs_per_day)
        return gen_load

    def generate_docs_employee_data(self, key_prefix ="employee_dataset", start=0, docs_per_day = 1, isShuffle = False):
        generators = []
        count = 1
        sys_admin_info = {"title" : "System Administrator and heliport manager",
                              "desc" : "...Last but not least, as the heliport manager, you will help maintain our growing fleet of remote controlled helicopters, that crash often due to inexperienced pilots.  As an independent thinker, you may be free to replace some of the technologies we currently use with ones you feel are better. If so, you should be prepared to discuss and debate the pros and cons of suggested technologies with other stakeholders",
                              "type" : "admin"}
        ui_eng_info = {"title" : "UI Engineer",
                           "desc" : "Couchbase server UI is one of the crown jewels of our product, which makes the Couchbase NoSQL database easy to use and operate, reports statistics on real time across large clusters, and much more. As a Member of Couchbase Technical Staff, you will design and implement front-end software for cutting-edge distributed, scale-out data infrastructure software systems, which is a pillar for the growing cloud infrastructure.",
                            "type" : "ui"}
        senior_arch_info = {"title" : "Senior Architect",
                               "desc" : "As a Member of Technical Staff, Senior Architect, you will design and implement cutting-edge distributed, scale-out data infrastructure software systems, which is a pillar for the growing cloud infrastructure. More specifically, you will bring Unix systems and server tech kung-fu to the team.",
                               "type" : "arch"}
        data_sets = self._shuffle([sys_admin_info, ui_eng_info, senior_arch_info],isShuffle)
        if end is None:
            end = self.docs_per_day
        join_yr = self._shuffle(range(2008, 2008 + self.years),isShuffle)
        join_mo = self._shuffle(range(1, self.months + 1),isShuffle)
        join_day = self._shuffle(range(1, self.days + 1),isShuffle)
        name = ["employee-%s-%s" % (key_prefix, str(i)) for i in xrange(start, end)]
        email = ["%s-mail@couchbase.com" % str(i) for i in xrange(start, end)]
        template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
        template += ' "email":"{4}", "job_title":"{5}", "type":"{6}", "desc":"{7}"}}'
        for info in data_sets:
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        random.seed(count)
                        prefix = str(random.random()*100000)
                        generators.append(
                            DocumentGenerator(key_prefix + prefix,
                                              template,
                                              name, [year], [month], [day],
                                              email, [info["title"]],
                                              [info["type"]], [info["desc"]],
                                              start=start, end=docs_per_day))
        return generators

    def generate_docs_using_monster(self,
            executatble_path=None, key_prefix="", bag_dir="lib/couchbase_helper/monster/bags",
            pod_name = None, num_items = 1, seed = None):
        "This method runs monster tool using localhost, creates a map of json based on a pattern"
        doc_list = list()
        command = executatble_path
        dest_path = "/tmp/{0}.txt".format(int(random.random()*1000))
        if pod_name is None:
            return doc_list
        else:
            pod_path = "lib/couchbase_helper/monster/prod/%s" % pod_name
        command += " -bagdir {0}".format(bag_dir)
        if seed is not None:
            command += " -s {0}".format(seed)
        command += " -n {0}".format(num_items)
        command += " -o {0}".format(dest_path)
        if pod_path is not None:
            command += " {0}".format(pod_path)
        # run command and generate temp file
        os.system(command)
        # read file and generate list
        with open(dest_path) as f:
            i= 1
            for line in f.readlines():
                key = "{0}{1}".format(key_prefix, i)
                data = json.loads(line[:len(line)-1])
                data["_id"] = key
                data["mutate"] = 0
                doc_list.append(data)
                i += 1
        os.remove(dest_path)
        return doc_list

    def _shuffle(self, data, isShuffle):
        if isShuffle:
            if not isinstance(data, list):
                data = [x for x in data]
            random.shuffle(data)
            return data
        return data
