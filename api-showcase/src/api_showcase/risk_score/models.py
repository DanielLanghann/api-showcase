from dataclasses import dataclass

@dataclass
class RiskMetrics:
    """Data class to hold calculated risk metrics"""
    total_questions: int
    questions_answered_no: int
    knockout_questions_answered_no: int
    overall_risk_score: int
    risk_percentage: float
    knockout_risk_percentage: float
    is_plausible: bool


