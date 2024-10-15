entity_source = {
  "type": "SingleTable",
  "entityId": "",
  "entityName": "fill this",
  "agentEntityId": "",
  "entityType": {
    "type": "Source",
    "maxItemsCountPerIteration": 1000,
    "maxMigrationItemsCountPerIteration": 1000,
    "pollingIntervalMilliseconds": 100
  },
  "agentId": "fill this",
  "customProperties": {},
  "tablesProperties": {
    "us-west-1.molo17": {}
  },
  "table": {
    "name": "fill this",
    "schema": "fill this"
  },
  "columns": [
    {
      "name": "quantity",
      "alias": "quantity",
      "type": "int"
    },
    {
      "name": "padding",
      "alias": "padding",
      "type": "string"
    },
    {
      "name": "num_sold",
      "alias": "num_sold",
      "type": "int"
    },
    {
      "name": "mutated",
      "alias": "mutated",
      "type": "int"
    },
    {
      "name": "product_name",
      "alias": "product_name",
      "type": "string"
    },
    {
      "name": "seller_verified",
      "alias": "seller_verified",
      "type": "boolean"
    },
    {
      "name": "avg_rating",
      "alias": "avg_rating",
      "type": "double"
    },
    {
      "name": "product_features",
      "alias": "product_features",
      "type": "any_array"
    },
    {
      "name": "seller_location",
      "alias": "seller_location",
      "type": "string"
    },
    {
      "name": "weight",
      "alias": "weight",
      "type": "double"
    },
    {
      "name": "product_image_links",
      "alias": "product_image_links",
      "type": "any_array"
    },
    {
      "name": "upload_date",
      "alias": "upload_date",
      "type": "string"
    },
    {
      "name": "template_name",
      "alias": "template_name",
      "type": "string"
    },
    {
      "name": "product_link",
      "alias": "product_link",
      "type": "string"
    },
    {
      "name": "product_category",
      "alias": "product_category",
      "type": "any_array"
    },
    {
      "name": "seller_name",
      "alias": "seller_name",
      "type": "string"
    },
    {
      "name": "product_reviews",
      "alias": "product_reviews",
      "type": "any_array"
    },
    {
      "name": "price",
      "alias": "price",
      "type": "double"
    },
    {
      "name": "product_specs",
      "alias": "product_specs",
      "type": "object"
    }
  ],
  "keys": [
    {
      "name": "product_name",
      "alias": "product_name",
      "type": "string"
    }
  ]
}

entity_target = {
  "type": "NoSqlEntity",
  "entityId": "",
  "entityName": "fill this",
  "agentEntityId": "",
  "entityType": {
    "type": "Target"
  },
  "agentId": "fill this",
  "customProperties": {},
  "tablesProperties": {
    "us-west-1.molo17": {}
  },
  "entityObject": {
    "scope": "",
    "collection": "fill this" # topic name
  },
  "columns": [
    {
      "name": "quantity",
      "type": "int"
    },
    {
      "name": "padding",
      "type": "string"
    },
    {
      "name": "num_sold",
      "type": "int"
    },
    {
      "name": "mutated",
      "type": "int"
    },
    {
      "name": "product_name",
      "type": "string"
    },
    {
      "name": "seller_verified",
      "type": "boolean"
    },
    {
      "name": "avg_rating",
      "type": "double"
    },
    {
      "name": "product_features",
      "type": "array"
    },
    {
      "name": "seller_location",
      "type": "string"
    },
    {
      "name": "weight",
      "type": "double"
    },
    {
      "name": "product_image_links",
      "type": "array"
    },
    {
      "name": "upload_date",
      "type": "string"
    },
    {
      "name": "template_name",
      "type": "string"
    },
    {
      "name": "product_link",
      "type": "string"
    },
    {
      "name": "product_category",
      "type": "array"
    },
    {
      "name": "seller_name",
      "type": "string"
    },
    {
      "name": "product_reviews",
      "type": "array"
    },
    {
      "name": "price",
      "type": "double"
    },
    {
      "name": "product_specs",
      "type": "map"
    }
  ],
  "keys": [
    {
      "name": "product_name",
      "type": "string"
    }
  ]
}