import pandas as pd
import uuid
import random
from datetime import timedelta

# ---------------- CONFIG ----------------
# Tune these down/up depending on your laptop
MAX_PURCHASE_SESSIONS = 10000      # delivered orders to convert into funnel sessions
NUM_NON_CONV_SESSIONS = 8000       # browsing-only sessions

PROGRESS_EVERY = 1000              # print progress every N sessions
random.seed(42)
# ----------------------------------------


def log(msg):
    print(msg, flush=True)


log("Loading Olist CSVs...")

orders = pd.read_csv("olist_orders_dataset.csv")
order_items = pd.read_csv("olist_order_items_dataset.csv")
products = pd.read_csv("olist_products_dataset.csv")
customers = pd.read_csv("olist_customers_dataset.csv")

log("Validating input shapes:")
log(f"  orders: {orders.shape}")
log(f"  order_items: {order_items.shape}")
log(f"  products: {products.shape}")
log(f"  customers: {customers.shape}")

# Keep essential columns
orders = orders[[
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp"
]]

order_items = order_items[["order_id", "product_id"]]
products = products[["product_id", "product_category_name"]]
customers = customers[[
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
]]

# Merge for lookups
order_items_sample = order_items.merge(products, on="product_id", how="left")
orders = orders.merge(customers, on="customer_id", how="left")

log("Filtering delivered orders...")

orders["order_purchase_timestamp"] = pd.to_datetime(
    orders["order_purchase_timestamp"],
    errors="coerce"
)
delivered_orders = orders[orders["order_status"] == "delivered"].dropna(
    subset=["order_purchase_timestamp"]
)

if delivered_orders.empty:
    raise RuntimeError("No delivered orders found. Check your CSV content.")

log(f"Delivered orders available: {len(delivered_orders)}")

# Limit to configured max
if len(delivered_orders) > MAX_PURCHASE_SESSIONS:
    delivered_orders = delivered_orders.sample(
        n=MAX_PURCHASE_SESSIONS,
        random_state=42
    )

product_ids = products["product_id"].dropna().unique().tolist()
customers_list = customers["customer_id"].dropna().unique().tolist()

device_types = ["desktop", "mobile", "tablet"]
traffic_sources = ["direct", "seo", "ads", "email", "social"]

events = []


def make_event(event_type,
               session_id,
               ts,
               customer_id=None,
               product_id=None,
               order_id=None,
               city=None,
               state=None,
               is_auth=None,
               traffic_source=None,
               device_type=None):
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "customer_id": customer_id if customer_id is not None else "",
        "event_type": event_type,
        "event_ts": ts.isoformat(sep=" "),
        "product_id": product_id if product_id is not None else "",
        "order_id": order_id if order_id is not None else "",
        "device_type": device_type or "",
        "traffic_source": traffic_source or "",
        "is_authenticated": 1 if is_auth else 0,
        "customer_city": city or "",
        "customer_state": state or ""
    }


# ---------- 1) Sessions ending in real purchases ----------
log("Generating purchase funnel sessions...")

for i, (_, row) in enumerate(delivered_orders.iterrows(), start=1):
    order_id = row["order_id"]
    customer_id = row["customer_id"]
    purchase_time = row["order_purchase_timestamp"]
    city = row.get("customer_city", None)
    state = row.get("customer_state", None)

    # all items for this order
    oi = order_items_sample[order_items_sample["order_id"] == order_id]
    if oi.empty:
        continue

    sess_id = str(uuid.uuid4())
    device = random.choice(device_types)
    source = random.choice(traffic_sources)
    is_auth = True  # for purchase, assume logged in

    # 1–3 products from order used in funnel
    oi_sample = oi.sample(n=min(3, len(oi)), random_state=None)

    # Start funnel slightly before purchase
    base_time = purchase_time - timedelta(minutes=random.randint(5, 40))

    # page_view
    events.append(make_event(
        "page_view", sess_id, base_time,
        customer_id=customer_id,
        city=city, state=state,
        is_auth=is_auth,
        traffic_source=source,
        device_type=device
    ))

    cur_time = base_time + timedelta(seconds=random.randint(5, 40))

    # view_product for sampled products
    for _, oi_row in oi_sample.iterrows():
        events.append(make_event(
            "view_product", sess_id, cur_time,
            customer_id=customer_id,
            product_id=oi_row["product_id"],
            city=city, state=state,
            is_auth=is_auth,
            traffic_source=source,
            device_type=device
        ))
        cur_time += timedelta(seconds=random.randint(5, 40))

    # add_to_cart
    events.append(make_event(
        "add_to_cart", sess_id, cur_time,
        customer_id=customer_id,
        product_id=oi_sample.iloc[0]["product_id"],
        city=city, state=state,
        is_auth=is_auth,
        traffic_source=source,
        device_type=device
    ))
    cur_time += timedelta(seconds=random.randint(5, 40))

    # checkout
    events.append(make_event(
        "checkout", sess_id, cur_time,
        customer_id=customer_id,
        city=city, state=state,
        is_auth=is_auth,
        traffic_source=source,
        device_type=device
    ))

    # purchase
    events.append(make_event(
        "purchase", sess_id, purchase_time,
        customer_id=customer_id,
        order_id=order_id,
        city=city, state=state,
        is_auth=is_auth,
        traffic_source=source,
        device_type=device
    ))

    if i % PROGRESS_EVERY == 0:
        log(f"  Processed {i} purchase sessions...")

log("Purchase sessions generation complete.")

# ---------- 2) Non-conversion browsing sessions ----------
log("Generating non-conversion browsing sessions...")

if delivered_orders.empty:
    base_orders_source = orders
else:
    base_orders_source = delivered_orders

for i in range(1, NUM_NON_CONV_SESSIONS + 1):
    base_order = base_orders_source.sample(1, random_state=None).iloc[0]
    base_time = base_order["order_purchase_timestamp"] - timedelta(
        days=random.randint(1, 60),
        minutes=random.randint(0, 1440)
    )

    sess_id = str(uuid.uuid4())
    device = random.choice(device_types)
    source = random.choice(traffic_sources)
    is_auth = random.random() < 0.4

    if is_auth and customers_list:
        customer_id = random.choice(customers_list)
        cust_row = customers[customers["customer_id"] == customer_id]
        if not cust_row.empty:
            cust_row = cust_row.iloc[0]
            city = cust_row["customer_city"]
            state = cust_row["customer_state"]
        else:
            city = state = None
    else:
        customer_id = None
        city = state = None

    steps = random.randint(2, 6)
    cur_time = base_time

    for _ in range(steps):
        r = random.random()
        if r < 0.4:
            etype = "page_view"
            pid = None
        elif r < 0.75:
            etype = "view_product"
            pid = random.choice(product_ids)
        else:
            etype = "add_to_cart"
            pid = random.choice(product_ids)

        events.append(make_event(
            etype,
            sess_id,
            cur_time,
            customer_id=customer_id,
            product_id=pid,
            city=city,
            state=state,
            is_auth=is_auth,
            traffic_source=source,
            device_type=device
        ))
        cur_time += timedelta(seconds=random.randint(5, 60))

    if i % PROGRESS_EVERY == 0:
        log(f"  Generated {i} non-conversion sessions...")

log("Non-conversion sessions generation complete.")
log(f"Total events created (before sort): {len(events)}")

# ---------- Finalize & write ----------
log("Converting to DataFrame & sorting...")

events_df = pd.DataFrame(events)
events_df.sort_values("event_ts", inplace=True)

out_file = "olist_clickstream_events.csv"
events_df.to_csv(out_file, index=False)

log(f"Done. Written {len(events_df)} rows to {out_file}")

