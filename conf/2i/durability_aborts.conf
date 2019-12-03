2i.indexscans_2i.SecondaryIndexingScanTests:
    # Primary index as view, indexes created before doc_aborts
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,gsi_type=plasma,sync_write_abort_pattern=all_aborts,GROUP=P0
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,gsi_type=plasma,sync_write_abort_pattern=initial_aborts,GROUP=P0
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,gsi_type=plasma,sync_write_abort_pattern=aborts_at_end,GROUP=P0

    # Primary index as GSI, indexes created before doc_aborts
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,use_gsi_for_primary=True,gsi_type=plasma,sync_write_abort_pattern=all_aborts,GROUP=P0
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,use_gsi_for_primary=True,gsi_type=plasma,sync_write_abort_pattern=initial_aborts,GROUP=P0
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,use_gsi_for_primary=True,gsi_type=plasma,sync_write_abort_pattern=aborts_at_end,GROUP=P0

    # Primary index as view, indexes created after doc_aborts
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,gsi_type=plasma,create_index_during=after_doc_ops,sync_write_abort_pattern=all_aborts,GROUP=P0
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,gsi_type=plasma,create_index_during=after_doc_ops,sync_write_abort_pattern=initial_aborts,GROUP=P0
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,gsi_type=plasma,create_index_during=after_doc_ops,sync_write_abort_pattern=aborts_at_end,GROUP=P0

    # Primary index as GSI, indexes created after doc_aborts
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,use_gsi_for_primary=True,create_index_during=after_doc_ops,gsi_type=plasma,sync_write_abort_pattern=all_aborts,GROUP=P0
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,use_gsi_for_primary=True,create_index_during=after_doc_ops,gsi_type=plasma,sync_write_abort_pattern=initial_aborts,GROUP=P0
    test_index_with_aborts,num_items=0,replicas=1,nodes_init=4,use_gsi_for_primary=True,create_index_during=after_doc_ops,gsi_type=plasma,sync_write_abort_pattern=aborts_at_end,GROUP=P0

