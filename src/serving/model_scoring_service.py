from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


ARTIFACT_DIR = Path("artifacts")
METADATA_PATH = ARTIFACT_DIR / "model_metadata.json"
CHURN_MODEL_PATH = ARTIFACT_DIR / "churn_model_xgb_v3_clean_weather.joblib"
CLV_MODEL_PATH = ARTIFACT_DIR / "clv_kmeans_v1.joblib"
CLV_SCALER_PATH = ARTIFACT_DIR / "clv_scaler_v1.joblib"
ENCODERS_PATH = ARTIFACT_DIR / "demographic_encoders.joblib"


class HouseholdFeatures(BaseModel):
    household_key: Optional[int] = None
    features: Dict[str, Any] = Field(default_factory=dict)


class BatchHouseholdFeatures(BaseModel):
    records: List[HouseholdFeatures]


class ModelArtifacts:
    def __init__(self) -> None:
        self.metadata = self._load_json(METADATA_PATH)
        self.churn_model = joblib.load(CHURN_MODEL_PATH)
        self.clv_model = joblib.load(CLV_MODEL_PATH)
        self.clv_scaler = joblib.load(CLV_SCALER_PATH)
        self.demographic_encoders = self._safe_load_joblib(ENCODERS_PATH)

        self.churn_features: List[str] = self.metadata["churn_features"]
        self.clv_features: List[str] = self.metadata["clv_features"]
        self.churn_threshold: float = float(self.metadata["churn_threshold"])
        self.churn_model_version: str = self.metadata["churn_model_version"]
        self.clv_label_map: Dict[str, str] = self.metadata["clv_label_map"]
        self.default_feature_values: Dict[str, Any] = self.metadata.get("default_feature_values", {})

    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f"Missing metadata file: {path}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _safe_load_joblib(path: Path) -> Any:
        if path.exists():
            return joblib.load(path)
        return None

    def _coerce_features(self, payload_features: Dict[str, Any]) -> pd.DataFrame:
        row = dict(self.default_feature_values)
        row.update(payload_features)

        missing = [f for f in set(self.churn_features + self.clv_features) if f not in row]
        if missing:
            raise HTTPException(status_code=422, detail=f"Missing required feature(s): {missing}")

        df = pd.DataFrame([row])

        if self.demographic_encoders:
            for col, encoder in self.demographic_encoders.items():
                if col in df.columns:
                    value = str(df.at[0, col])
                    classes = set(map(str, encoder.classes_))
                    if value not in classes:
                        fallback = "Unknown" if "Unknown" in classes else list(classes)[0]
                        value = fallback
                    df[col] = encoder.transform([value])

        return df

    def score_one(self, record: HouseholdFeatures) -> Dict[str, Any]:
        df = self._coerce_features(record.features)

        churn_prob = float(self.churn_model.predict_proba(df[self.churn_features])[:, 1][0])
        churn_label = "At Risk" if churn_prob >= self.churn_threshold else "Active"

        clv_scaled = self.clv_scaler.transform(df[self.clv_features])
        clv_cluster = int(self.clv_model.predict(clv_scaled)[0])
        clv_segment = self.clv_label_map[str(clv_cluster)]

        return {
            "household_key": record.household_key,
            "churn_risk_score": round(churn_prob, 6),
            "churn_label": churn_label,
            "clv_cluster": clv_cluster,
            "clv_segment": clv_segment,
            "model_version": self.churn_model_version,
            "threshold": self.churn_threshold,
        }


app = FastAPI(title="Household Scoring Service", version="1.0.0")
artifacts: Optional[ModelArtifacts] = None


@app.on_event("startup")
def load_artifacts() -> None:
    global artifacts
    artifacts = ModelArtifacts()


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/metadata")
def metadata() -> Dict[str, Any]:
    if artifacts is None:
        raise HTTPException(status_code=503, detail="Artifacts not loaded")
    return {
        "churn_model_version": artifacts.churn_model_version,
        "threshold": artifacts.churn_threshold,
        "churn_feature_count": len(artifacts.churn_features),
        "clv_feature_count": len(artifacts.clv_features),
    }


@app.post("/score/household")
def score_household(payload: HouseholdFeatures) -> Dict[str, Any]:
    if artifacts is None:
        raise HTTPException(status_code=503, detail="Artifacts not loaded")
    return artifacts.score_one(payload)


@app.post("/score/batch")
def score_batch(payload: BatchHouseholdFeatures) -> Dict[str, Any]:
    if artifacts is None:
        raise HTTPException(status_code=503, detail="Artifacts not loaded")
    results = [artifacts.score_one(record) for record in payload.records]
    return {"count": len(results), "results": results}
