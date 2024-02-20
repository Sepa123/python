from pydantic import BaseModel
from datetime import datetime
from typing import Optional,List,Any 



class AttachedDocument(BaseModel):
    date: Optional[datetime]
    attachedDocumentType: Optional[int]
    attachedDocumentName: Optional[str]
    attachedDocumentNumber: Optional[str]
    attachedDocumentTotal: Optional[int]
    documentTypeId: Optional[str]
    folio: Optional[int]
    reason: None
    gloss: Optional[str]

class InfAnalysis(BaseModel):
    accountNumber: Optional[str]
    businessCenter: Optional[str]
    classifier01: Optional[str]
    classifier02: Optional[str]

class DocumentTax(BaseModel):
    taxeCode1: Optional[str]
    taxePercentaje1: Optional[int]
    taxeValue1: Optional[int]
    taxeCode2: Optional[str]
    taxePercentaje2: Optional[int]
    taxeValue2: Optional[int]
    taxeCode3: Optional[str]
    taxePercentaje3: Optional[int]
    taxeValue3: Optional[int]
    taxeCode4: Optional[str]
    taxePercentaje4: Optional[int]
    taxeValue4: Optional[int]
    taxeCode5: Optional[str]
    taxePercentaje5: Optional[int]
    taxeValue5: Optional[int]


class Detail(BaseModel):
    detailLine: Optional[int]
    type: Optional[str]
    code: Optional[int]
    count: Optional[int]
    price: Optional[int]
    isExempt: None
    discountType: Optional[str]
    discountValue: Optional[int]
    comment: Optional[str]
    analysis: Optional[str]
    total: Optional[int]
    priceList: Optional[int]
    infAnalysis: Optional[InfAnalysis]

class ExportDatum(BaseModel):
    exportBillingRate: Optional[int]
    exportBillingCoinID: Optional[str]
    totalExport: None
    exemptExport: None
    destinationCountry: Optional[str]
    destinationMerchandise: Optional[str]
    landingPort: Optional[str]
    saleClause: Optional[str]
    saleMode: Optional[str]
    shipmentPort: Optional[str]
    totalClause: Optional[int]
    transportWay: Optional[str]


class VoucherInfo(BaseModel):
    folio: Optional[int]
    year: Optional[int]
    type: Optional[str]


class listaVenta (BaseModel):
    documentType: Optional[str]
    firstFolio: Optional[int]
    lastFolio: Optional[int]
    status: Optional[str]
    emissionDate: Optional[datetime]
    dateTime: Optional[datetime]
    expirationDate: Optional[datetime]
    clientFile: Optional[str]
    contactIndex: Optional[str]
    paymentCondition: Optional[str]
    sellerFileId: Optional[str]
    billingCoin: Optional[str]
    billingRate: Optional[int]
    shopId: Optional[str]
    priceList: Optional[int]
    giro: Optional[str]
    city: Optional[str]
    district: Optional[str]
    contact: Optional[int]
    attachedDocuments: Optional[List[AttachedDocument]]
    details: Optional[List[Detail]]
    gloss: Optional[str]
    affectableTotal: Optional[int]
    exemptTotal: Optional[int]
    taxeCode: Optional[str]
    taxeValue: Optional[int]
    documentTaxes: Optional[List[DocumentTax]]
    ventaRecDesGlobal: None
    total: Optional[int]
    voucherInfo: Optional[List[VoucherInfo]]
    inventoryInfo: Optional[List[Any]]
    customFields: Optional[List[Any]]
    exportData: Optional[List[ExportDatum]]
    isTransferDocument: Optional[str]
    timestamp: None




class Main(BaseModel):
    success: bool
    message: str
    exceptionMessage: None
    totalItems: int
    pageNumber: int
    itemsPerPage: int
    saleList: Optional[List[listaVenta]]