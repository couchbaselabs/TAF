rbac_data = dict()
for user in ["cluster_admin", "bucket_admin", "rbac_admin",
             "guest", "user_query", "user_manager", "user_booking",
             "review_writer", "bucket_data_writer",
             "airline_admin", "airline_booking",
             "hotel_admin", "hotel_booking", "cbas_admin"]:
    rbac_data[user] = dict()
    rbac_data[user]["auth"] = [user, user + "_passwd_!@#"]

# RBAC  user - roles
rbac_data["cluster_admin"]["roles"] = "cluster_admin"
rbac_data["bucket_admin"]["roles"] = "bucket_admin[travel-sample]," \
                                     "query_manage_index[travel-sample]"
rbac_data["rbac_admin"]["roles"] = "security_admin_local"
rbac_data["bucket_data_writer"]["roles"] = "data_writer[travel-sample]," \
                                           "query_select[travel-sample]"

rbac_data["guest"]["roles"] = "query_select[travel-sample]"
rbac_data["user_query"]["roles"] = "query_select[travel-sample]," \
                                   "query_update[travel-sample]"
rbac_data["user_manager"]["roles"] = "data_reader[travel-sample]," \
                                     "data_writer[travel-sample]," \
                                     "query_select[travel-sample]"
rbac_data["user_booking"]["roles"] = "data_reader[travel-sample]," \
                                     "data_writer[travel-sample]"
rbac_data["review_writer"]["roles"] = "data_reader[travel-sample]," \
                                      "data_writer[travel-sample]"
rbac_data["airline_admin"]["roles"] = "data_reader[travel-sample]," \
                                      "data_writer[travel-sample]"
rbac_data["airline_booking"]["roles"] = "data_reader[travel-sample]," \
                                        "data_writer[travel-sample]," \
                                        "query_select[travel-sample]," \
                                        "query_update[travel-sample],"
rbac_data["hotel_admin"]["roles"] = "data_reader[travel-sample]," \
                                    "data_writer[travel-sample]"
rbac_data["hotel_booking"]["roles"] = "data_reader[travel-sample]," \
                                      "data_writer[travel-sample]"
rbac_data["cbas_admin"]["roles"] = "analytics_admin"
