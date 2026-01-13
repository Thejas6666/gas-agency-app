from flask import Blueprint, render_template, request, redirect, url_for, flash
from sqlalchemy import text
from app.db.session import SessionLocal

iocl_movements_bp = Blueprint("iocl_movements", __name__)


@iocl_movements_bp.route("/iocl-movements", methods=["GET", "POST"])
def iocl_view():
    db = SessionLocal()
    try:
        # 1. Fetch current active OPEN stock day
        open_day = db.execute(text("""
            SELECT stock_day_id, stock_date 
            FROM stock_days 
            WHERE status = 'OPEN' 
            ORDER BY stock_date DESC LIMIT 1
        """)).fetchone()

        if not open_day:
            flash("No active OPEN stock day found.", "error")
            return redirect(url_for("stock_day.dashboard"))

        s_id = open_day.stock_day_id

        # 2. MASTER LOCK CHECK: Check the explicit is_reconciled flag
        is_finalized = db.execute(text("""
            SELECT COALESCE(MAX(is_reconciled), 0) FROM daily_stock_summary 
            WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).scalar() == 1

        # 3. Check if "No Movement" flag is set for Step 2
        current_no_mov = db.execute(text("""
            SELECT COALESCE(MAX(iocl_no_movement), 0) FROM daily_stock_summary 
            WHERE stock_day_id = :s_id
        """), {"s_id": s_id}).scalar() or 0

        # 4. Step 1 Prerequisite Check
        step1_done = db.execute(text("""
            SELECT COUNT(*) FROM daily_stock_summary 
            WHERE stock_day_id = :s_id AND opening_filled IS NOT NULL
        """), {"s_id": s_id}).scalar() > 0

        is_editable = step1_done and not is_finalized

        # 5. Handle Form Submission
        if request.method == "POST":
            if not is_editable:
                flash("Entry Locked: This day has been finalized in Step 4.", "danger")
                return redirect(url_for("iocl_movements.iocl_view"))

            no_mov_checked = 1 if request.form.get("no_movement") else 0

            if no_mov_checked == 1:
                db.execute(text("""
                    UPDATE daily_stock_summary 
                    SET item_receipt = 0, item_return = 0, iocl_no_movement = 1
                    WHERE stock_day_id = :s_id
                """), {"s_id": s_id})
            else:
                for key, value in request.form.items():
                    if key.startswith("receipt_"):
                        c_id = key.split("_")[1]
                        receipt = int(value or 0)
                        ret = int(request.form.get(f"return_{c_id}", 0))

                        db.execute(text("""
                            UPDATE daily_stock_summary 
                            SET item_receipt = :receipt, item_return = :ret, iocl_no_movement = 0
                            WHERE stock_day_id = :s_id AND cylinder_type_id = :c_id
                        """), {"receipt": receipt, "ret": ret, "s_id": s_id, "c_id": c_id})

            db.commit()
            flash("IOCL Movements updated successfully.", "success")
            return redirect(url_for("iocl_movements.iocl_view"))

        # 6. Fetch values for UI
        rows = db.execute(text("""
            SELECT 
                ct.cylinder_type_id,
                ct.code AS cylinder_type,
                COALESCE(dss.item_receipt, 0) AS item_receipt,
                COALESCE(dss.item_return, 0) AS item_return
            FROM cylinder_types ct
            JOIN daily_stock_summary dss ON dss.cylinder_type_id = ct.cylinder_type_id
            WHERE dss.stock_day_id = :s_id
            ORDER BY ct.cylinder_type_id
        """), {"s_id": s_id}).fetchall()

        total_received = sum(row.item_receipt for row in rows)
        total_returned = sum(row.item_return for row in rows)
        has_data = (total_received + total_returned) > 0 or current_no_mov == 1

        return render_template("iocl_movements.html",
                               rows=rows, stock_date=open_day.stock_date,
                               no_movement=current_no_mov, has_data=has_data,
                               is_editable=is_editable, step1_done=step1_done,
                               is_finalized=is_finalized, total_received=total_received,
                               total_returned=total_returned)
    finally:
        db.close()


@iocl_movements_bp.route("/iocl-movements/delete", methods=["POST"])
def delete_movements():
    db = SessionLocal()
    try:
        open_day = db.execute(text("SELECT stock_day_id FROM stock_days WHERE status = 'OPEN' LIMIT 1")).fetchone()
        if open_day:
            s_id = open_day.stock_day_id

            # Master Lock Check
            is_finalized = db.execute(text("""
                SELECT COALESCE(MAX(is_reconciled), 0) FROM daily_stock_summary 
                WHERE stock_day_id = :s_id
            """), {"s_id": s_id}).scalar() == 1

            if is_finalized:
                flash("Locked: Cannot reset finalized records.", "danger")
                return redirect(url_for("iocl_movements.iocl_view"))

            db.execute(text("""
                UPDATE daily_stock_summary 
                SET item_receipt = 0, item_return = 0, iocl_no_movement = 0
                WHERE stock_day_id = :s_id
            """), {"s_id": s_id})
            db.commit()
            flash("Records and flags reset successfully.", "info")
        return redirect(url_for("iocl_movements.iocl_view"))
    finally:
        db.close()