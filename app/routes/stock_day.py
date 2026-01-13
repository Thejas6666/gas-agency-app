from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import text
from datetime import date, timedelta, datetime
from app.db.session import SessionLocal

stock_day_bp = Blueprint("stock_day", __name__)


@stock_day_bp.route("/")
@stock_day_bp.route("/dashboard")
@login_required
def dashboard():
    db = SessionLocal()
    try:
        # 1. Fetch the most recent day (OPEN or CLOSED)
        day = db.execute(text("""
            SELECT stock_day_id, stock_date, status, delivery_no_movement 
            FROM stock_days 
            ORDER BY stock_date DESC 
            LIMIT 1
        """)).fetchone()

        # 2. Fetch all CLOSED days for History
        history = db.execute(text("""
            SELECT stock_day_id, stock_date 
            FROM stock_days 
            WHERE status = 'CLOSED' 
            ORDER BY stock_date DESC
        """)).fetchall()

        is_day_closed = (day.status.upper() == 'CLOSED') if day else False

        # Initialize progress flags (Sequential Logic)
        progress = {
            "opening_stock": False,
            "iocl_movements": False,
            "deliveries": False,
            "finalized_stock": False,
            "expected_cash": False,
            "cash_collection": False,
            "reconciled_cash": False
        }

        if day and not is_day_closed:
            s_id = day.stock_day_id

            # Step 1: Opening Stock
            progress["opening_stock"] = db.execute(text("""
                SELECT COUNT(*) FROM daily_stock_summary 
                WHERE stock_day_id = :s_id AND opening_filled IS NOT NULL
            """), {"s_id": s_id}).scalar() > 0

            # Step 2: IOCL Movements
            # Done if receipts exist OR "No Movement" toggle was saved
            iocl_status = db.execute(text("""
                SELECT 
                    (COALESCE(SUM(item_receipt + item_return), 0) > 0) OR 
                    (MAX(CAST(iocl_no_movement AS UNSIGNED)) = 1)
                FROM daily_stock_summary 
                WHERE stock_day_id = :s_id
            """), {"s_id": s_id}).fetchone()

            has_iocl_logic = bool(iocl_status[0]) if iocl_status else False
            progress["iocl_movements"] = has_iocl_logic and progress["opening_stock"]

            # Step 3: Delivery Issues
            # Done if rows exist in delivery_issues OR "delivery_no_movement" flag is set in stock_days
            has_delivery_data = db.execute(text("""
                SELECT COUNT(*) FROM delivery_issues WHERE stock_day_id = :s_id
            """), {"s_id": s_id}).scalar() > 0

            no_delivery_movement = (day.delivery_no_movement == 1)
            progress["deliveries"] = (has_delivery_data or no_delivery_movement) and progress["iocl_movements"]

            # Step 4: Reconciliation (Closing Stock)
            # FIXED: We now check the explicit 'is_reconciled' flag.
            # This allows the step to be "Completed" even if sales_regular is 0.
            has_finalized = db.execute(text("""
                SELECT MAX(is_reconciled) 
                FROM daily_stock_summary 
                WHERE stock_day_id = :s_id
            """), {"s_id": s_id}).scalar() == 1

            progress["finalized_stock"] = has_finalized and progress["deliveries"]

            # Steps 5, 6, 7 (Cash Handling)
            has_exp = db.execute(text("SELECT COUNT(*) FROM delivery_expected_amount WHERE stock_day_id = :s_id"),
                                 {"s_id": s_id}).scalar() > 0
            progress["expected_cash"] = has_exp and progress["finalized_stock"]

            has_coll = db.execute(text("SELECT COUNT(*) FROM delivery_cash_deposit WHERE stock_day_id = :s_id"),
                                  {"s_id": s_id}).scalar() > 0
            progress["cash_collection"] = has_coll and progress["expected_cash"]

            has_recon = db.execute(text("SELECT COUNT(*) FROM delivery_cash_balance WHERE stock_day_id = :s_id"),
                                   {"s_id": s_id}).scalar() > 0
            progress["reconciled_cash"] = has_recon and progress["cash_collection"]

        return render_template("dashboard.html",
                               day=day,
                               history=history,
                               progress=progress,
                               is_day_closed=is_day_closed,
                               user=current_user)
    finally:
        db.close()


@stock_day_bp.route("/generate-report", methods=["POST"])
@login_required
def generate_report():
    db = SessionLocal()
    try:
        report_type = request.form.get("report_type")
        selected_date = request.form.get("selected_date")

        record = db.execute(text("""
            SELECT stock_day_id, stock_date FROM stock_days 
            WHERE stock_date = :sd AND status = 'CLOSED'
        """), {"sd": selected_date}).fetchone()

        if not record:
            flash(f"No finalized records found for {selected_date}", "warning")
            return redirect(url_for('stock_day.dashboard'))

        date_str = record.stock_date.strftime('%Y-%m-%d') if hasattr(record.stock_date, 'strftime') else str(
            record.stock_date)

        if report_type == 'stock':
            return redirect(url_for('cash_reconciliation.download_stock', day_id=record.stock_day_id, date=date_str))
        else:
            return redirect(url_for('cash_reconciliation.download_cash', day_id=record.stock_day_id, date=date_str))
    finally:
        db.close()


@stock_day_bp.route("/create-stock-day", methods=["GET", "POST"])
@login_required
def create_new_day():
    db = SessionLocal()
    try:
        today_val = date.today().isoformat()
        last_day = db.execute(text("SELECT stock_date FROM stock_days ORDER BY stock_date DESC LIMIT 1")).fetchone()

        if last_day:
            last_dt = last_day.stock_date if isinstance(last_day.stock_date, date) else datetime.strptime(
                str(last_day.stock_date), '%Y-%m-%d').date()
            next_available = (last_dt + timedelta(days=1)).isoformat()
        else:
            next_available = today_val

        if request.method == "POST":
            selected_date = request.form.get("stock_date")
            exists = db.execute(text("SELECT 1 FROM stock_days WHERE stock_date = :sd"),
                                {"sd": selected_date}).fetchone()
            if exists:
                flash(f"Error: Date {selected_date} already exists!", "danger")
                return redirect(url_for('stock_day.create_new_day'))

            # Initialize new day with no_movement flag as 0
            db.execute(
                text("INSERT INTO stock_days (stock_date, status, delivery_no_movement) VALUES (:sd, 'OPEN', 0)"),
                {"sd": selected_date})
            db.commit()
            return redirect(url_for('stock_day.dashboard'))

        return render_template("create_stock_day.html", next_available_date=next_available, today=today_val)
    finally:
        db.close()