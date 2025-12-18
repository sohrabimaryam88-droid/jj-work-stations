
from flask import Flask, request, send_file, jsonify, after_this_request
from pydantic import ValidationError
from schema.schemas import WncsRequest
import zipfile
import tempfile
from pathlib import Path
from utiles.wncs import Wncs
import os
from validators.wncs_validator import (validate_zip_contains_msmt,validate_excel_columns)
from db.mongo import DataBase





from dotenv import load_dotenv
load_dotenv() 
app = Flask(__name__)

MONGO_URL=os.getenv("MONGO_URL")
DbARAS=os.getenv("dumpDbARAS")
outputfile=os.getenv("outputfile")
WNCS_DB=os.getenv("WNCS_DB")
WNCS_COLLECTION=os.getenv("WNCS_COLLECTION")



@app.route("/ping", methods=["GET"])
def ping():
    return "pong"


@app.route("/wncs", methods=["POST"])
def run_wncs():
    try:
        print("🚀 Request received")
        rowfile = request.files.get("rowfile")
        cell_status = request.files.get("cell_status")
        print("📦 Files received")

        if not rowfile or not cell_status:
            return jsonify({"error": "rowfile and cell_status files are required"}), 400
        WncsRequest(
            rowfile_name=rowfile.filename,
            cell_status_name=cell_status.filename
        )

        #save file in TemporaryDirectory
        with tempfile.TemporaryDirectory() as tmpdir:
            print("📂 Temp directory for inputs created")
            rowfile_path = Path(tmpdir) / rowfile.filename
            cellstatus_path = Path(tmpdir) / cell_status.filename

            rowfile.save(rowfile_path)
            cell_status.save(cellstatus_path)
            print("💾 Input files saved")
            validate_zip_contains_msmt(rowfile_path)
            validate_excel_columns(cellstatus_path)
            # proccessing WNCS
            df = Wncs(
                dumpDbARAS=DbARAS,
                mongoDbDataBaseUrl=MONGO_URL,
                path_cellstatus=cellstatus_path,
                path_rowfilezip=rowfile_path
            )
            print("✅ WNCS processing finished")

        # # Save the output CSV to the system temporary directory
        output_path = Path(tempfile.gettempdir()) / outputfile
        df.to_csv(output_path, index=False)

        dbwncs=DataBase(uri=MONGO_URL, db_name=WNCS_DB)
        wncscollection=dbwncs[WNCS_COLLECTION]
        records=df.to_dict(orient="records")
        if records:
            wncscollection.insert_many(records)
            print(f"{len(records)} is inserted to MongoDB")

        print(f"📤 CSV generated at {output_path}")

        # Delete the CSV file after sending the response
        @after_this_request
        def remove_file(response):
            try:
                os.remove(output_path)
                print("🗑 Temporary CSV deleted")
            except Exception as e:
                print(f"⚠️ Could not delete temp CSV: {e}")
            return response

        #send file to user 
        return send_file(
            output_path,
            as_attachment=True,
            download_name=outputfile
        )
    except ValidationError as ve:
        return jsonify({
            "error": "Invalid request",
            "details": ve.errors()
        }), 422
    
    except ValueError as ve:
        return jsonify({
            "error": str(ve)
        }), 422

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500




@app.route("/wncs/<CELLNAME_CELL>", methods=["GET"])
def selectwncs(CELLNAME_CELL):
    try:
        dbwncs=DataBase(uri=MONGO_URL, db_name=WNCS_DB)
        query={"CELLNAME_CELL":CELLNAME_CELL}

        data = dbwncs.get(WNCS_COLLECTION,query=query, fields={"_id": 0})
        return jsonify({
            "count": len(data),
            "data": data
        }), 200

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500        



@app.route("/wncs/update/<CELLNAME_CELL>",methods=["PUT"])
def updatewncs(CELLNAME_CELL):
    try:
        data = request.get_json()
        if not data:
            return jsonify ({"No data provided for update"}),400
        dbwncs=DataBase(uri=MONGO_URL,db_name=WNCS_DB)
        query={"CELLNAME_CELL":CELLNAME_CELL}
        result=dbwncs.update(collection=WNCS_COLLECTION,query=query,new_values=data)
        return jsonify({
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "message": "Update completed successfully"
        }), 200

    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500


@app.route("/wncs/remove/<CELLNAME_CELL>",methods=["DELETE"])
def removewncs(CELLNAME_CELL):
    try:
        dbwncs=DataBase(uri=MONGO_URL,db_name=WNCS_DB)
        query={"CELLNAME_CELL":CELLNAME_CELL}
        result=dbwncs.delete(collection=WNCS_COLLECTION,query=query)
        return jsonify({
            "deleted_count": result.deleted_count,
            "message": "Delete completed successfully"
        }), 200

    except Exception as e:
            return jsonify({
                "error": "Internal server error",
                "details": str(e)
            }), 500


if __name__ == "__main__":
    app.run(debug=True)
