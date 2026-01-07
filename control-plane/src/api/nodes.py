from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, AnyHttpUrl
from sqlalchemy.orm import Session
from datetime import datetime

from src.db.session import get_db
from src.db.models import Node

router = APIRouter(prefix="/nodes", tags=["nodes"])


class NodeRegister(BaseModel):
    name: str
    base_url: AnyHttpUrl


@router.post("/register")
def register_node(payload: NodeRegister, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == payload.name).first()
    if existing:
        existing.base_url = str(payload.base_url)
        existing.status = "healthy"
        existing.last_heartbeat = datetime.utcnow().isoformat()
        db.commit()
        return {"message": "updated", "node": {"name": existing.name, "base_url": existing.base_url}}

    node = Node(name=payload.name, base_url=str(payload.base_url))
    db.add(node)
    db.commit()
    return {"message": "registered", "node": {"name": node.name, "base_url": node.base_url}}


@router.get("")
def list_nodes(db: Session = Depends(get_db)):
    nodes = db.query(Node).all()
    return [
        {
            "name": n.name,
            "base_url": n.base_url,
            "status": n.status,
            "last_heartbeat": n.last_heartbeat,
        }
        for n in nodes
    ]
