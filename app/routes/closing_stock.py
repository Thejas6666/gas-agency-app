from flask import Blueprint, render_template, request, redirect, url_for, flash
from sqlalchemy import text
from app.db.session import SessionLocal

closing_stock_bp = Blueprint("closing_stock", __name__)

@closing_stock_bp.route("/closing-stock", methods=["GET", "POST"])
def closing_view():
    db = SessionLocal()
    try:
        # 1. Fetch the current active OPEN stock day
        open_day = db.execute(text("""
            SELECT stock_day_id, stock_date, delivery_no_movement 
            FROM stock_days 
            WHERE status = 'OPEN' 
            ORDER BY stock_date DESC LIMIT 1
        """)).fetchone()

        if not open_day:
            flash("No active OPEN stock day found.", "danger")
            return redirect(url_for("stock_day.dashboard"))

        s_id = open_day.stock_day_id

        # 2. PREREQUISITE CHECK:
        # Step 3 is done if: (Audit rows exist) OR (No Movement flag is enabled)
        has_delivery_rows = db.execute(text("""
            SELECT COUNT(*) FROM delivery_issues WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).scalar() > 0

        step3_done = has_delivery_rows or (open_day.delivery_no_movement == 1)

        # 3. MASTER LOCK CHECK:
        # Using the new is_reconciled flag instead of SUM(sales)
        is_finalized = db.execute(text("""
            SELECT MAX(is_reconciled) 
            FROM daily_stock_summary 
            WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).scalar() == 1

        # 4. Fetch Data for Reconciliation Math
        summary_raw = db.execute(text("""
            SELECT s.*, t.code 
            FROM daily_stock_summary s
            JOIN cylinder_types t ON s.cylinder_type_id = t.cylinder_type_id
            WHERE s.stock_day_id = :s_id
            ORDER BY t.cylinder_type_id
        """), {"s_id": s_id}).fetchall()

        issues_raw = db.execute(text("""
            SELECT cylinder_type_id, 
                   SUM(regular_qty) as total_reg, 
                   SUM(nc_qty) as total_nc, 
                   SUM(dbc_qty) as total_dbc, 
                   SUM(tv_out_qty) as total_tv
            FROM delivery_issues 
            WHERE stock_day_id = :s_id 
            GROUP BY cylinder_type_id
        """), {"s_id": s_id}).fetchall()

        issues_map = {r.cylinder_type_id: r for r in issues_raw}

        display_data = []
        for s in summary_raw:
            iss = issues_map.get(s.cylinder_type_id)
            reg = iss.total_reg if iss else 0
            nc = iss.total_nc if iss else 0
            dbc = iss.total_dbc if iss else 0
            tv = iss.total_tv if iss else 0

            # Reconciliation Formulas
            calc_filled = (s.opening_filled or 0) + (s.item_receipt or 0) - (reg + nc + dbc)
            calc_empty = (s.opening_empty or 0) + reg + tv - (s.item_return or 0)
            defective = s.defective_empty_vehicle or 0
            total_stock = calc_filled + calc_empty + defective

            display_data.append({
                'cylinder_type_id': s.cylinder_type_id,
                'code': s.code,
                'opening': {'f': s.opening_filled, 'e': s.opening_empty},
                'iocl': {'in': s.item_receipt, 'out': s.item_return},
                'issues': {'reg': reg, 'nc': nc, 'dbc': dbc},
                'tv': tv,
                'defective_v': defective,
                'closing': {'f': calc_filled, 'e': calc_empty},
                'total_stock': total_stock
            })

        # 5. Handle Finalization (POST)
        if request.method == "POST":
            if is_finalized:
                flash("This day is already finalized.", "warning")
                return redirect(url_for("closing_stock.closing_view"))

            if not step3_done:
                flash("Error: Please complete Step 3 before finalizing.", "danger")
                return redirect(url_for("closing_stock.closing_view"))

            for item in display_data:
                db.execute(text("""
                    UPDATE daily_stock_summary 
                    SET closing_filled = :cf, 
                        closing_empty = :ce, 
                        total_stock = :ts,
                        sales_regular = :sr, 
                        nc_qty = :nq, 
                        dbc_qty = :dq, 
                        tv_out_qty = :tvq,
                        is_reconciled = 1
                    WHERE stock_day_id = :s_id AND cylinder_type_id = :ct_id
                """), {
                    "cf": item['closing']['f'],
                    "ce": item['closing']['e'],
                    "ts": item['total_stock'],
                    "sr": item['issues']['reg'],
                    "nq": item['issues']['nc'],
                    "dq": item['issues']['dbc'],
                    "tvq": item['tv'],
                    "s_id": s_id,
                    "ct_id": item['cylinder_type_id']
                })

            db.commit()
            flash(f"Reconciliation successful. Stock locked for {open_day.stock_date}.", "success")
            return redirect(url_for("closing_stock.closing_view"))

        return render_template("closing_stock.html",
                               stock_date=open_day.stock_date,
                               data=display_data,
                               is_finalized=is_finalized,
                               step3_done=step3_done)
    finally:
        db.close()