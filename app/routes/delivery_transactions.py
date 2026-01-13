from flask import Blueprint, render_template, request, redirect, url_for, flash
from sqlalchemy import text
from app.db.session import SessionLocal

delivery_transactions_bp = Blueprint("delivery_transactions", __name__)

def get_open_day(db):
    return db.execute(text("""
        SELECT stock_day_id, stock_date, delivery_no_movement 
        FROM stock_days WHERE status = 'OPEN' LIMIT 1
    """)).fetchone()

@delivery_transactions_bp.route("/delivery-transactions", methods=["GET", "POST"])
def transactions_view():
    db = SessionLocal()
    try:
        open_day = get_open_day(db)
        if not open_day:
            flash("No active OPEN stock day found.", "danger")
            return redirect(url_for("stock_day.dashboard"))

        s_id = open_day.stock_day_id

        # UPDATED MASTER LOCK: Check the explicit is_reconciled flag
        is_finalized = db.execute(text("""
            SELECT COALESCE(MAX(is_reconciled), 0) FROM daily_stock_summary 
            WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).scalar() == 1

        if request.method == "POST":
            if is_finalized:
                flash("Locked: Reconciliation (Step 4) is complete.", "danger")
                return redirect(url_for("delivery_transactions.transactions_view"))

            # 1. HANDLE RESET ALL BUTTON
            if "reset_db" in request.form:
                db.execute(text("DELETE FROM delivery_issues WHERE stock_day_id = :s_id"), {"s_id": s_id})
                db.execute(text("UPDATE daily_stock_summary SET tv_out_qty = 0 WHERE stock_day_id = :s_id"), {"s_id": s_id})
                db.execute(text("UPDATE stock_days SET delivery_no_movement = 0 WHERE stock_day_id = :s_id"), {"s_id": s_id})
                db.commit()
                flash("Records cleared successfully.", "info")
                return redirect(url_for("delivery_transactions.transactions_view"))

            # 2. HANDLE NO MOVEMENT TOGGLE
            no_mov_checked = 1 if request.form.get("delivery_no_movement") else 0
            db.execute(text("UPDATE stock_days SET delivery_no_movement = :val WHERE stock_day_id = :s_id"),
                       {"val": no_mov_checked, "s_id": s_id})

            if no_mov_checked == 1:
                db.execute(text("DELETE FROM delivery_issues WHERE stock_day_id = :s_id"), {"s_id": s_id})
                db.execute(text("UPDATE daily_stock_summary SET tv_out_qty = 0 WHERE stock_day_id = :s_id"), {"s_id": s_id})
            else:
                # 3. PROCESS STANDARD DATA
                data_map = {}
                for key, value in request.form.items():
                    if key.startswith("issue_"):
                        qty = int(value or 0)
                        parts = key.split("_")
                        b_id, t_id, cat = parts[1], parts[2], parts[3]
                        if (b_id, t_id) not in data_map:
                            data_map[(b_id, t_id)] = {'r': 0, 'n': 0, 'd': 0, 'tv': 0}

                        mapping = {'REFILL': 'r', 'NC': 'n', 'DBC': 'd', 'TVOUT': 'tv'}
                        if cat in mapping:
                            data_map[(b_id, t_id)][mapping[cat]] = qty

                for (b_id, t_id), q in data_map.items():
                    if q['r'] > 0 or q['n'] > 0 or q['d'] > 0 or q['tv'] > 0:
                        db.execute(text("""
                            INSERT INTO delivery_issues 
                                (stock_day_id, delivery_boy_id, cylinder_type_id, regular_qty, nc_qty, dbc_qty, tv_out_qty, delivery_source)
                            VALUES (:s_id, :b_id, :t_id, :r, :n, :d, :tv, 'DELIVERY_BOY')
                            ON DUPLICATE KEY UPDATE 
                                regular_qty = VALUES(regular_qty), nc_qty = VALUES(nc_qty), 
                                dbc_qty = VALUES(dbc_qty), tv_out_qty = VALUES(tv_out_qty)
                        """), {"s_id": s_id, "b_id": b_id, "t_id": t_id, "r": q['r'], "n": q['n'], "d": q['d'], "tv": q['tv']})

                # 4. SYNC TOTALS TO SUMMARY TABLE
                db.execute(text("""
                    UPDATE daily_stock_summary dss
                    SET tv_out_qty = (
                        SELECT COALESCE(SUM(tv_out_qty), 0) 
                        FROM delivery_issues 
                        WHERE stock_day_id = dss.stock_day_id AND cylinder_type_id = dss.cylinder_type_id
                    ) WHERE stock_day_id = :s_id
                """), {"s_id": s_id})

            db.commit()
            flash("Delivery transactions updated successfully.", "success")
            return redirect(url_for("delivery_transactions.transactions_view"))

        # Fetch data for UI
        boys = db.execute(text("SELECT delivery_boy_id, name FROM delivery_boys WHERE is_active = 1 ORDER BY name")).fetchall()
        types = db.execute(text("SELECT cylinder_type_id, code FROM cylinder_types ORDER BY code")).fetchall()
        issues_raw = db.execute(text("SELECT * FROM delivery_issues WHERE stock_day_id = :s_id"), {"s_id": s_id}).fetchall()
        issues = {(r.delivery_boy_id, r.cylinder_type_id): r for r in issues_raw}

        is_saved = (len(issues_raw) > 0 or open_day.delivery_no_movement == 1)

        return render_template("delivery_transactions.html",
                               boys=boys, types=types, issues=issues,
                               is_saved=is_saved, no_movement=open_day.delivery_no_movement,
                               is_finalized=is_finalized, stock_date=open_day.stock_date)
    finally:
        db.close()