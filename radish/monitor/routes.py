from __future__ import annotations

from flask import Flask, current_app, jsonify, redirect, render_template, url_for


def register_routes(app: Flask) -> None:
    @app.route("/")
    def index():
        return redirect(url_for("workers_page"))

    @app.route("/workers")
    def workers_page():
        service = current_app.config["MONITOR_SERVICE"]
        data = service.get_workers_page_data()
        return render_template("workers.html", data=data)

    @app.route("/worker-overview")
    def worker_overview_page():
        service = current_app.config["MONITOR_SERVICE"]
        data = service.get_worker_overview_data()
        return render_template("worker_overview.html", data=data)

    @app.route("/tasks")
    def tasks_page():
        service = current_app.config["MONITOR_SERVICE"]
        data = service.get_tasks_page_data()
        return render_template("tasks.html", data=data)

    @app.route("/api/workers")
    def workers_api():
        service = current_app.config["MONITOR_SERVICE"]
        data = service.get_workers_page_data()
        return jsonify(data)

    @app.route("/api/worker-overview")
    def worker_overview_api():
        service = current_app.config["MONITOR_SERVICE"]
        data = service.get_worker_overview_data()
        return jsonify(data)

    @app.route("/api/tasks")
    def tasks_api():
        service = current_app.config["MONITOR_SERVICE"]
        data = service.get_tasks_page_data()
        return jsonify(data)