#!/usr/bin/env bash
# End-to-end walkthrough of one order. Run after `docker compose up --build -d` has settled.
set -e
id() { sed -E 's/.*"(id|orderId)":"([^"]+)".*/\2/'; }
J='Content-Type: application/json'

REST=$(curl -s -X POST localhost:9092/restaurants -H "$J" -d '{"name":"Windhoek Grill","address":"Independence Ave"}' | id)
ITEM=$(curl -s -X POST localhost:9092/restaurants/$REST/menu -H "$J" -d '{"name":"Burger","price":55.00}' | id)
CUST=$(curl -s -X POST localhost:9091/customers -H "$J" -d "{\"name\":\"Test User\",\"email\":\"t$RANDOM@example.com\",\"phone\":\"0811234567\"}" | id)
ORDER=$(curl -s -X POST localhost:9093/orders -H "$J" -d "{\"customerId\":\"$CUST\",\"restaurantId\":\"$REST\",\"deliveryAddress\":\"12 Main St\",\"items\":[{\"menuItemId\":\"$ITEM\",\"name\":\"Burger\",\"quantity\":2,\"price\":55.00}]}" | id)
echo "Order $ORDER created"; sleep 4
curl -s localhost:9093/orders/$ORDER; echo
curl -s -X PUT localhost:9092/restaurants/orders/$ORDER/preparing; echo; sleep 2
curl -s -X PUT localhost:9092/restaurants/orders/$ORDER/ready; echo; sleep 3
curl -s -X PUT localhost:9095/deliveries/$ORDER/start; echo; sleep 2
curl -s -X PUT localhost:9095/deliveries/$ORDER/complete; echo; sleep 3
echo "--- final order";        curl -s localhost:9093/orders/$ORDER; echo
echo "--- notifications";      curl -s "localhost:9096/notifications?orderId=$ORDER"; echo
echo "--- customer history";   curl -s localhost:9091/customers/$CUST/orders; echo
echo "--- admin summary";      curl -s localhost:9097/admin/stats/summary; echo
