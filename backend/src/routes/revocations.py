from fastapi import APIRouter,Depends
from src.middlewares.auth import current,writer
from src.schemas.business import RevokeInput
from src.services import revocations
from src.controllers.business import purge_revocation
router=APIRouter()
@router.get('/revocations')
def listing(u=Depends(current)):return revocations.list_items()
@router.post('/revocations')
def purge(data:RevokeInput,u=Depends(writer)):return purge_revocation(data,u)
