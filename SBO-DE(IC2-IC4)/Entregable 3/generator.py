import json
import random
from datetime import datetime, timedelta

def generate_mock_data(count=1000):
    pos_data = []
    ecomm_data = []
    erp_data = []
    
    for i in range(count):
        # 1. Decidir si es venta de Tienda o Web
        is_pos = random.choice([True, False])
        trans_id = f"POS-{random.randint(100,999)}-{i}" if is_pos else f"WEB-{random.randint(1000,9999)}"
        amount = round(random.uniform(10.0, 500.0), 2)
        
        # Simular el 40% de estatus 'RETURNED'
        status = "RETURNED" if random.random() < 0.40 else "COMPLETED"
        timestamp = (datetime.now() - timedelta(minutes=random.randint(1, 60))).strftime("%Y-%m-%d %H:%M:%S")


        record = {
            "transaction_id": trans_id,
            "total_amount": amount,
            "status": status,
            "event_timestamp": timestamp,
            "customer_email": f"user_{i}@example.com" if not is_pos else None,
            "store_id": f"STORE-{random.randint(1,8)}" if is_pos else "ONLINE"
        }

        if is_pos: pos_data.append(record)
        else: ecomm_data.append(record)

        # Generar el registro del ERP (Con un 5% de probabilidad de que el ERP no lo vea)
        if random.random() > 0.05:
            erp_data.append({
                "reference_id": trans_id,
                "amount_credited": amount if status == "COMPLETED" else 0, # El ERP a veces solo registra lo neto
                "gl_account": "101-SALES",
                "sync_date": timestamp
            })

    # Guardar archivos
    with open('pos_sales.json', 'w') as f:
        for entry in pos_data: f.write(json.dumps(entry) + '\n')
    
    with open('ecomm_sales.json', 'w') as f:
        for entry in ecomm_data: f.write(json.dumps(entry) + '\n')
        
    with open('erp_inventory.json', 'w') as f:
        for entry in erp_data: f.write(json.dumps(entry) + '\n')

    print(f"¡Listo! Generados {len(pos_data)} POS, {len(ecomm_data)} E-comm y {len(erp_data)} ERP records.")

generate_mock_data(1000)