queries = {
  "query_1" : "select l.l_returnflag, l.l_linestatus, sum(l.l_quantity) as sum_qty,\nsum(l.l_extendedprice) as sum_base_price,\nsum(l.l_extendedprice * (1 - l.l_discount)) as sum_disc_price,\nsum(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) as sum_charge,\navg(l.l_quantity) as avg_qty, avg(l.l_extendedprice) as avg_price,\navg(l.l_discount) as avg_disc, count(*) as count_order\nfrom lineitem l where l.l_shipdate <= '1998-09-16'\ngroup by l.l_returnflag, l.l_linestatus\norder by l.l_returnflag, l.l_linestatus;",
  "query_2" : "select s.s_acctbal,\ns.s_name,\nn.n_name,\np.p_partkey,\np.p_mfgr,\ns.s_address,\ns.s_phone,\ns.s_comment\nfrom part p,\nsupplier s,\npartsupp ps,\nnation n,\nregion r\nwhere p.p_partkey = ps.ps_partkey\nand s.s_suppkey = ps.ps_suppkey\nand p.p_size = 37\nand p.p_type like '%COPPER'\nand s.s_nationkey = n.n_nationkey\nand n.n_regionkey = r.r_regionkey\nand r.r_name = 'EUROPE'\nand ps.ps_supplycost = (select VALUE min(ps2.ps_supplycost)\nfrom part p2,\npartsupp ps2,\nsupplier s2,\nnation n2,\nregion r2\nwhere p2.p_partkey = ps2.ps_partkey\nand p2.p_partkey = p.p_partkey\nand s2.s_suppkey = ps2.ps_suppkey\nand s2.s_nationkey = n2.n_nationkey\nand n2.n_regionkey = r2.r_regionkey\nand r2.r_name = 'EUROPE')[0] order by s.s_acctbal desc,\nn.n_name,\ns.s_name,\np.p_partkey limit 100;",
  "query_3" : "select l.l_orderkey,\nsum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\no.o_orderdate,\no.o_shippriority\nfrom customer c,\norders o,\nlineitem l\nwhere c.c_mktsegment  = 'BUILDING'\nand c.c_custkey = o.o_custkey\nand l.l_orderkey = o.o_orderkey\nand o.o_orderdate  < '1995-03-22'\nand l.l_shipdate  > '1995-03-22' group by l.l_orderkey,\no.o_orderdate,\no.o_shippriority order by revenue desc,\no.o_orderdate limit 10;",
  "query_4" : "select o.o_orderpriority,\ncount(*) as order_count\nfrom orders as o\nwhere o.o_orderdate >= '1996-05-01'\nand o.o_orderdate < '1996-08-01'\nand exists ( select *\nfrom lineitem l\nwhere l.l_orderkey = o.o_orderkey\nand l.l_commitdate < l.l_receiptdate )\ngroup by o.o_orderpriority\norder by o.o_orderpriority;",
  "query_5" : "select n.n_name,\nsum(l.l_extendedprice * (1 - l.l_discount)) as revenue\nfrom customer c,\norders o,\nlineitem l,\nsupplier s,\nnation n,\nregion r\nwhere c.c_custkey = o.o_custkey\nand l.l_orderkey = o.o_orderkey\nand l.l_suppkey = s.s_suppkey\nand c.c_nationkey  = s.s_nationkey\nand s.s_nationkey = n.n_nationkey\nand c.c_nationkey = n.n_nationkey\nand n.n_regionkey = r.r_regionkey\nand r.r_name  = 'AFRICA'\nand o.o_orderdate  >= '1993-01-01'\nand o.o_orderdate < '1994-01-01'\ngroup by n.n_name\norder by revenue desc;",
  "query_6" : "",
  "query_7" : "select supp_nation,\ncust_nation,\nl_year,\nsum(volume) as revenue\nfrom ( select n1.n_name as supp_nation,\nn2.n_name as cust_nation,\nDATE_PART_STR(l.l_shipdate,\n'year') as l_year,\nl.l_extendedprice * (1 - l.l_discount) as volume\nfrom supplier s,\nlineitem l,\norders o,\ncustomer c,\nnation n1 ,\nnation n2\nwhere s.s_suppkey = l.l_suppkey\nand o.o_orderkey = l.l_orderkey\nand c.c_custkey = o.o_custkey\nand s.s_nationkey = n1.n_nationkey\nand c.c_nationkey = n2.n_nationkey\nand ( (n1.n_name = 'KENYA' and n2.n_name  = 'PERU')\nor (n1.n_name  = 'PERU' and n2.n_name  = 'KENYA') )\nand l.l_shipdate  >=  '1995-01-01'\nand l.l_shipdate  <= '1996-12-31' ) as shipping\ngroup by\n    supp_nation,\n    cust_nation,\n    l_year\norder by\n    supp_nation,\n    cust_nation,\n    l_year;",
  "query_8" : "select o_year,\nsum(case when nation = 'PERU' then volume else 0 end) / sum(volume) as mkt_share\nfrom ( select DATE_PART_STR(o.o_orderdate,\n'year') as o_year,\nl.l_extendedprice * (1 - l.l_discount) as volume,\nn2.n_name as nation\nfrom part p,\nsupplier s,\nlineitem l,\norders o,\ncustomer c,\nnation n1,\nnation n2,\nregion r\nwhere p.p_partkey = l.l_partkey\nand s.s_suppkey = l.l_suppkey\nand l.l_orderkey = o.o_orderkey\nand o.o_custkey = c.c_custkey\nand c.c_nationkey = n1.n_nationkey\nand n1.n_regionkey = r.r_regionkey\nand r.r_name  = 'AMERICA'\nand s.s_nationkey = n2.n_nationkey\nand o.o_orderdate  between '1995-01-01'\nand '1996-12-31'\nand p.p_type = 'ECONOMY BURNISHED NICKEL' ) as all_nations\ngroup by o_year\norder by o_year;",
  "query_9" : "select count (1)\nfrom part p,\nsupplier s,\nlineitem l,\npartsupp ps,\norders o,\nnation n\nwhere s.s_suppkey = l.l_suppkey\nAND ps.ps_suppkey = l.l_suppkey\nAND s.s_suppkey = ps.ps_suppkey\nAND ps.ps_partkey = l.l_partkey\nAND p.p_partkey = l.l_partkey\nAND p.p_partkey = ps.ps_partkey\nAND o.o_orderkey = l.l_orderkey\nAND s.s_nationkey = n.n_nationkey\nand p.p_name  < 'be';",
  "query_10" : "select c.c_custkey,\nc.c_name,\nsum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\nc.c_acctbal,\nn_name,\nc.c_address,\nc.c_phone,\nc.c_comment\nfrom customer c,\norders o,\nlineitem l,\nnation n\nwhere c.c_custkey = o.o_custkey\nand l.l_orderkey = o.o_orderkey\nand o.o_orderdate >= '1993-07-01'\nand o.o_orderdate < '1993-10-01'\nand l.l_returnflag  = 'R'\nand c.c_nationkey = n.n_nationkey\ngroup by c.c_custkey,\nc.c_name,\nc.c_acctbal,\nc.c_phone,\nn.n_name,\nc.c_address,\nc.c_comment\norder by revenue desc limit 20;"
}

kv_indexes = {
  "region": [
    "create index r_regionkey_idx_region on region (r_regionkey);",
    "create index r_name_idx_region on region (r_name);",
    "create index r_comment_idx_region on region (r_comment);"
  ],
  "nation": [
    "create index n_nationkey_idx_nation on nation (n_nationkey);",
    "create index n_name_idx_nation on nation (n_name);",
    "create index n_regionkey_idx_nation on nation (n_regionkey);",
    "create index n_comment_idx_nation on nation (n_comment);"
  ],
  "supplier": [
    "create index s_suppkey_idx_supplier on supplier (s_suppkey);",
    "create index s_name_idx_supplier on supplier (s_name);",
    "create index s_address_idx_supplier on supplier (s_address);",
    "create index s_nationkey_idx_supplier on supplier (s_nationkey);",
    "create index s_phone_idx_supplier on supplier (s_phone);",
    "create index s_acctbal_idx_supplier on supplier (s_acctbal);",
    "create index s_comment_idx_supplier on supplier (s_comment);"
  ],
  "customer": [
    "create index c_custkey_idx_customer on customer (c_custkey);",
    "create index c_name_idx_customer on customer (c_name);",
    "create index c_address_idx_customer on customer (c_address);",
    "create index c_nationkey_idx_customer on customer (c_nationkey);",
    "create index c_phone_idx_customer on customer (c_phone);",
    "create index c_acctbal_idx_customer on customer (c_acctbal);",
    "create index c_mktsegment_idx_customer on customer (c_mktsegment);",
    "create index c_comment_idx_customer on customer (c_comment);"
  ],
  "part": [
    "create index p_partkey_idx_part on part(p_partkey);",
    "create index p_name_idx_part on part(p_name);",
    "create index p_mfgr_idx_part on part(p_mfgr);",
    "create index p_brand_idx_part on part(p_brand);",
    "create index p_type_idx_part on part(p_type);",
    "create index p_size_idx_part on part(p_size);",
    "create index p_container_idx_part on part(p_container);",
    "create index p_retailprice_idx_part on part(p_retailprice);",
    "create index p_comment_idx_part on part(p_comment);"
  ],
  "partsupp": [
    "create index ps_partkey_idx_partsupp on partsupp (ps_partkey);",
    "create index ps_suppkey_idx_partsupp on partsupp (ps_suppkey);",
    "create index ps_availqty_idx_partsupp on partsupp (ps_availqty);",
    "create index ps_supplycost_idx_partsupp on partsupp (ps_supplycost);",
    "create index ps_comment_idx_partsupp on partsupp (ps_comment);"
  ],
  "orders": [
    "create index o_orderkey_idx_orders on orders (o_orderkey);",
    "create index o_custkey_idx_orders on orders (o_custkey);",
    "create index o_orderstatus_idx_orders on orders (o_orderstatus);",
    "create index o_totalprice_idx_orders on orders (o_totalprice);",
    "create index o_orderdate_idx_orders on orders (o_orderdate);",
    "create index o_orderpriority_idx_orders on orders (o_orderpriority);",
    "create index o_clerk_idx_orders on orders (o_clerk);",
    "create index o_shippriority_idx_orders on orders (o_shippriority);",
    "create index o_comment_idx_orders on orders (o_comment);"
  ],
  "lineitem": [
    "create index l_orderkey_idx_lineitem on lineitem (l_orderkey);",
    "create index l_partkey_idx_lineitem on lineitem (l_partkey);",
    "create index l_suppkey_idx_lineitem on lineitem (l_suppkey);",
    "create index l_linenumber_idx_lineitem on lineitem (l_linenumber);",
    "create index l_quantity_idx_lineitem on lineitem (l_quantity);",
    "create index l_extendedprice_idx_lineitem on lineitem (l_extendedprice);",
    "create index l_discount_idx_lineitem on lineitem (l_discount);",
    "create index l_tax_idx_lineitem on lineitem (l_tax);",
    "create index l_returnflag_idx_lineitem on lineitem (l_returnflag);",
    "create index l_linestatus_idx_lineitem on lineitem (l_linestatus);",
    "create index l_shipdate_idx_lineitem on lineitem (l_shipdate);",
    "create index l_commitdate_idx_lineitem on lineitem (l_commitdate);",
    "create index l_receiptdate_idx_lineitem on lineitem (l_receiptdate);",
    "create index l_shipinstruct_idx_lineitem on lineitem (l_shipinstruct);",
    "create index l_shipmode_idx_lineitem on lineitem (l_shipmode);",
    "create index l_comment_idx_lineitem on lineitem (l_comment);"
  ]
}

cbas_indexes = {
  "region": [
    {
      "index_name": "r_regionkey_idx_region",
      "indexed_field": ["r_regionkey:bigint"]
    },
    {
      "index_name": "r_name_idx_region",
      "indexed_field": ["r_name:string"]
    },
    {
      "index_name": "r_comment_idx_region",
      "indexed_field": ["r_comment:string"]
    }
  ],
  "nation": [
    {
      "index_name": "n_nationkey_idx_nation",
      "indexed_field": ["n_nationkey:bigint"]
    },
    {
      "index_name": "n_name_idx_nation",
      "indexed_field": ["n_name:string"]
    },
    {
      "index_name": "n_regionkey_idx_nation",
      "indexed_field": ["n_regionkey:bigint"]
    },
    {
      "index_name": "n_comment_idx_nation",
      "indexed_field": ["n_comment:string"]
    }
  ],
  "supplier": [
    {
      "index_name": "s_suppkey_idx_supplier",
      "indexed_field": ["s_suppkey:bigint"]
    },
    {
      "index_name": "s_name_idx_supplier",
      "indexed_field": ["s_name:string"]
    },
    {
      "index_name": "s_address_idx_supplier",
      "indexed_field": ["s_address:string"]
    },
    {
      "index_name": "s_nationkey_idx_supplier",
      "indexed_field": ["s_nationkey:bigint"]
    },
    {
      "index_name": "s_phone_idx_supplier",
      "indexed_field": ["s_phone:string"]
    },
    {
      "index_name": "s_acctbal_idx_supplier",
      "indexed_field": ["s_acctbal:double"]
    },
    {
      "index_name": "s_comment_idx_supplier",
      "indexed_field": ["s_comment:string"]
    }
  ],
  "customer": [
    {
      "index_name": "c_custkey_idx_customer",
      "indexed_field": ["c_custkey:bigint"]
    },
    {
      "index_name": "c_name_idx_customer",
      "indexed_field": ["c_name:string"]
    },
    {
      "index_name": "c_address_idx_customer",
      "indexed_field": ["c_address:string"]
    },
    {
      "index_name": "c_nationkey_idx_customer",
      "indexed_field": ["c_nationkey:bigint"]
    },
    {
      "index_name": "c_phone_idx_customer",
      "indexed_field": ["c_phone:string"]
    },
    {
      "index_name": "c_acctbal_idx_customer",
      "indexed_field": ["c_acctbal:double"]
    },
    {
      "index_name": "c_mktsegment_idx_customer",
      "indexed_field": ["c_mktsegment:string"]
    },
    {
      "index_name": "c_comment_idx_customer",
      "indexed_field": ["c_comment:string"]
    }
  ],
  "part": [
    {
      "index_name": "p_partkey_idx_part",
      "indexed_field": ["p_partkey:bigint"]
    },
    {
      "index_name": "p_name_idx_part",
      "indexed_field": ["p_name:string"]
    },
    {
      "index_name": "p_mfgr_idx_part",
      "indexed_field": ["p_mfgr:string"]
    },
    {
      "index_name": "p_brand_idx_part",
      "indexed_field": ["p_brand:string"]
    },
    {
      "index_name": "p_type_idx_part",
      "indexed_field": ["p_type:string"]
    },
    {
      "index_name": "p_size_idx_part",
      "indexed_field": ["p_size:bigint"]
    },
    {
      "index_name": "p_container_idx_part",
      "indexed_field": ["p_container:string"]
    },
    {
      "index_name": "p_retailprice_idx_part",
      "indexed_field": ["p_retailprice:double"]
    },
    {
      "index_name": "p_comment_idx_part",
      "indexed_field": ["p_comment:string"]
    }
  ],
  "partsupp": [
    {
      "index_name": "ps_partkey_idx_partsupp",
      "indexed_field": ["ps_partkey:bigint"]
    },
    {
      "index_name": "ps_suppkey_idx_partsupp",
      "indexed_field": ["ps_suppkey:bigint"]
    },
    {
      "index_name": "ps_availqty_idx_partsupp",
      "indexed_field": ["ps_availqty:bigint"]
    },
    {
      "index_name": "ps_supplycost_idx_partsupp",
      "indexed_field": ["ps_supplycost:double"]
    },
    {
      "index_name": "ps_comment_idx_partsupp",
      "indexed_field": ["ps_comment:string"]
    }
  ],
  "orders": [
    {
      "index_name": "o_orderkey_idx_orders",
      "indexed_field": ["o_orderkey:bigint"]
    },
    {
      "index_name": "o_custkey_idx_orders",
      "indexed_field": ["o_custkey:string"]
    },
    {
      "index_name": "o_orderstatus_idx_orders",
      "indexed_field": ["o_orderstatus:string"]
    },
    {
      "index_name": "o_totalprice_idx_orders",
      "indexed_field": ["o_totalprice:double"]
    },
    {
      "index_name": "o_orderdate_idx_orders",
      "indexed_field": ["o_orderdate:string"]
    },
    {
      "index_name": "o_orderpriority_idx_orders",
      "indexed_field": ["o_orderpriority:string"]
    },
    {
      "index_name": "o_clerk_idx_orders",
      "indexed_field": ["o_clerk:string"]
    },
    {
      "index_name": "o_shippriority_idx_orders",
      "indexed_field": ["o_shippriority:bigint"]
    },
    {
      "index_name": "o_comment_idx_orders",
      "indexed_field": ["o_comment:string"]
    }
  ],
  "lineitem": [
    {
      "index_name": "l_orderkey_idx_lineitem",
      "indexed_field": ["l_orderkey:bigint"]
    },
    {
      "index_name": "l_partkey_idx_lineitem",
      "indexed_field": ["l_partkey:bigint"]
    },
    {
      "index_name": "l_suppkey_idx_lineitem",
      "indexed_field": ["l_suppkey:bigint"]
    },
    {
      "index_name": "l_linenumber_idx_lineitem",
      "indexed_field": ["l_linenumber:bigint"]
    },
    {
      "index_name": "l_quantity_idx_lineitem",
      "indexed_field": ["l_quantity:bigint"]
    },
    {
      "index_name": "l_extendedprice_idx_lineitem",
      "indexed_field": ["l_extendedprice:double"]
    },
    {
      "index_name": "l_discount_idx_lineitem",
      "indexed_field": ["l_discount:double"]
    },
    {
      "index_name": "l_tax_idx_lineitem",
      "indexed_field": ["l_tax:double"]
    },
    {
      "index_name": "l_returnflag_idx_lineitem",
      "indexed_field": ["l_returnflag:string"]
    },
    {
      "index_name": "l_linestatus_idx_lineitem",
      "indexed_field": ["l_linestatus:string"]
    },
    {
      "index_name": "l_shipdate_idx_lineitem",
      "indexed_field": ["l_shipdate:string"]
    },
    {
      "index_name": "l_commitdate_idx_lineitem",
      "indexed_field": ["l_commitdate:string"]
    },
    {
      "index_name": "l_receiptdate_idx_lineitem",
      "indexed_field": ["l_receiptdate:string"]
    },
    {
      "index_name": "l_shipinstruct_idx_lineitem",
      "indexed_field": ["l_shipinstruct:string"]
    },
    {
      "index_name": "l_shipmode_idx_lineitem",
      "indexed_field": ["l_shipmode:string"]
    },
    {
      "index_name": "l_comment_idx_lineitem",
      "indexed_field": ["l_comment:string"]
    }
  ]
}

bucket_doc_count = {
  "region": 5,
  "nation": 25,
  "supplier": 1000,
  "customer": 15000,
  "part": 20000,
  "partsupp": 80000,
  "orders": 150000,
  "lineitem": 600572
}

data_file_info = {
  "region": {
      "filename": "region.tbl",
      "key_generator": "r_regionkey",
  },
  "nation": {
      "filename": "nation.tbl",
      "key_generator": "n_nationkey",
  },
  "supplier": {
      "filename": "supplier.tbl",
      "key_generator": "s_suppkey",
  },
  "customer": {
      "filename": "customer.tbl",
      "key_generator": "c_custkey",
  },
  "part": {
      "filename": "part.tbl",
      "key_generator": "p_partkey",
  },
  "partsupp": {
      "filename": "partsupp.tbl",
      "key_generator": "sno",
  },
  "orders": {
      "filename": "orders.tbl",
      "key_generator": "o_orderkey",
  },
  "lineitem": {
      "filename": "lineitem.tbl",
      "key_generator": "sno",
  }
}