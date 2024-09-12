import logging
import os
from fhir import resources as fr
from pydicom import dcmread, dataset, config
from datetime import datetime
from . import fhirutils

config.convert_wrong_length_to_UN = True
LOGGER = logging.getLogger('pynetdicom')

from dotenv import load_dotenv
load_dotenv()

def dcm_coded_concept(CodeSequence):
    """Generate coded concept from DICOM CodeSequence."""
    concepts = [
        {
            "code": seq[0x0008, 0x0100].value,
            "system": seq[0x0008, 0x0102].value,
            "display": seq[0x0008, 0x0104].value,
        }
        for seq in CodeSequence
    ]
    return concepts


def add_instance(study, series, ds, fp):
    """Add an instance to the ImagingStudy series."""
    instanceUID = ds.SOPInstanceUID

    if series.instance:
        selected_instance = next((i for i in series.instance if i.uid == instanceUID), None)
        if selected_instance:
            LOGGER.error("Error: SOP Instance UID is not unique")
            return

    init_instance = {
        "uid": instanceUID,
        "sopClass": fhirutils.gen_instance_sopclass(ds.SOPClassUID),
        "number": ds.InstanceNumber,
        "title": get_instance_title(series, ds),
    }

    selected_instance = fr.imagingstudy.ImagingStudySeriesInstance(**init_instance)
    series.instance = series.instance or []
    series.instance.append(selected_instance)
    study.numberOfInstances += 1
    series.numberOfInstances += 1


def get_instance_title(series, ds):
    """Generate the title for the instance."""
    try:
        if series.modality.code == "SR":
            return ds.ConceptNameCodeSequence[0x0008, 0x0104].value
        return '\\'.join(ds.ImageType)
    except (AttributeError, KeyError):
        LOGGER.warning("Unable to set instance title, setting default Image Type")
        return '\\'.join(['ORIGINAL', 'PRIMARY'])


def add_series(study, ds, fp):
    """Add a series to the ImagingStudy."""
    seriesInstanceUID = ds.SeriesInstanceUID

    if study.series:
        selected_series = next((s for s in study.series if s.uid == seriesInstanceUID), None)
        if selected_series:
            add_instance(study, selected_series, ds, fp)
            return

    # Create new series if not found
    init_series = {
        "uid": seriesInstanceUID,
        "modality": fhirutils.gen_modality_coding(ds.Modality),
        "description": get_series_description(ds),
        "number": ds.SeriesNumber,
        "numberOfInstances": 0,
    }

    series = fr.imagingstudy.ImagingStudySeries(**init_series)
    fhirutils.update_study_modality_list(study, series.modality)
    series.started = get_series_start_time(ds)

    study.series = study.series or []
    study.series.append(series)
    study.numberOfSeries += 1

    add_instance(study, series, ds, fp)


def get_series_description(ds):
    """Fetch the series description, default to 'No Description' if not found."""
    return getattr(ds, 'SeriesDescription', "No Description")


def get_series_start_time(ds):
    """Generate the start time for the series."""
    now = datetime.now()
    series_time = getattr(ds, 'SeriesTime', now.strftime("%H%M%S"))
    series_date = getattr(ds, 'SeriesDate', now.strftime("%Y%m%d"))
    LOGGER.info(f"Series Date Time: {series_date} {series_time}")
    return fhirutils.gen_started_datetime(series_date, series_time)


def create_imaging_study(ds, fp, imagingStudyID, serviceRequestID, patientID):
    """Create a new ImagingStudy FHIR resource."""
    init_data = {
        "status": "available",
        "subject": {"reference": f"Patient/{patientID}"},
        "basedOn": [{"reference": f"ServiceRequest/{serviceRequestID}"}],
    }
    study = fr.imagingstudy.ImagingStudy(**init_data)
    study.id = imagingStudyID or study.id
    study.description = get_study_description(ds)
    study.identifier = [
        fhirutils.gen_accession_identifier(ds.AccessionNumber),
        fhirutils.gen_studyinstanceuid_identifier(ds.StudyInstanceUID),
    ]
    study.reasonCode = fhirutils.gen_reason(None, None)  # Placeholder for reason code

    # Study initialization
    study.numberOfSeries = 0
    study.numberOfInstances = 0

    add_series(study, ds, fp)
    return study


def get_study_description(ds):
    """Fetch study description, default to 'No Description' if not found."""
    try:
        return ds.StudyDescription
    except AttributeError:
        LOGGER.error("Study Description is missing")
        return "No Description"


def process_dicom_to_fhir(dcm_dir, imagingStudyID, serviceRequestID, patientID):
    """Process DICOM files and convert to FHIR ImagingStudy resource."""
    files = [os.path.join(r, file) for r, _, f in os.walk(dcm_dir) for file in f if '.dcm' in file]

    imaging_study = None
    studyInstanceUID = None

    try:
        for fp in files:
            with dcmread(fp, force=True) as ds:
                if studyInstanceUID is None:
                    studyInstanceUID = ds.StudyInstanceUID

                if studyInstanceUID != ds.StudyInstanceUID:
                    raise ValueError("Incorrect DICOM path, more than one study detected")

                if imaging_study is None:
                    imaging_study = create_imaging_study(ds, fp, imagingStudyID, serviceRequestID, patientID)
                else:
                    add_series(imaging_study, ds, fp)

    except Exception as err:
        LOGGER.error(f"Error processing DICOM to FHIR: {err}")

    return imaging_study
