class BasePipeline:
    """
    Base class for ETL pipelines.
    Provides standard structure for extract, transform, load operations.
    """
    
    def __init__(self, client_id, data_type, parameters=None):
        self.tracker = PipelineTracker()
        self.run_id = None
        self.client_id = client_id
        self.data_type = data_type
        self.parameters = parameters or {}
    
    def start(self):
        """Start the pipeline and get a run ID"""
        self.run_id = self.tracker.start_pipeline_run(
            pipeline_name=self.__class__.__name__,
            client_id=self.client_id,
            data_type=self.data_type,
            parameters=self.parameters
        )
        return self.run_id
    
    def extract(self):
        """Extract data from source systems"""
        raise NotImplementedError("Subclasses must implement extract()")
    
    def transform(self):
        """Transform the extracted data"""
        raise NotImplementedError("Subclasses must implement transform()")
    
    def load(self):
        """Load the transformed data to destination"""
        raise NotImplementedError("Subclasses must implement load()")
    
    def run(self):
        """Execute the full pipeline"""
        try:
            self.start()
            self.extract()
            self.transform()
            self.load()
            
            self.tracker.complete_pipeline_run(
                self.run_id, 
                "success"
            )
            
            return {
                "run_id": self.run_id,
                "status": "success"
            }
        except Exception as e:
            if self.run_id:
                self.tracker.complete_pipeline_run(
                    self.run_id, 
                    "failed",
                    error_details=str(e)
                )
            logging.error(f"Pipeline failed: {str(e)}")
            raise


class OrderPipeline(BasePipeline):
    """Pipeline for processing order data"""
    
    def __init__(self, client_id, date_from=None, date_to=None):
        parameters = {
            "date_from": date_from,
            "date_to": date_to
        }
        super().__init__(client_id, "orders", parameters)
        
        # Load config
        self.config = {
            "staging_table": "orders_staging",
            "target_table": "orders_main",
            "unique_key": "order_id",
            "compare_fields": ["updated_at", "status", "amount"]
        }
    
    def extract(self):
        """Extract order data from source system"""
        step_id = self.tracker.start_pipeline_step(self.run_id, "extract_from_source")
        
        try:
            # Your extraction logic here
            # ...
            
            self.tracker.complete_pipeline_step(
                step_id,
                status="success",
                record_count=record_count,
                output_location=raw_data_path
            )
            
            return raw_data_path
            
        except Exception as e:
            self.tracker.complete_pipeline_step(
                step_id,
                status="failed",
                error_message=str(e)
            )
            raise
    
    def transform(self):
        """Transform raw order data"""
        step_id = self.tracker.start_pipeline_step(self.run_id, "transform_from_raw")
        
        try:
            # Your transformation logic here
            # ...
            
            self.tracker.complete_pipeline_step(
                step_id,
                status="success",
                record_count=record_count,
                output_location=processed_data_path
            )
            
            return processed_data_path
            
        except Exception as e:
            self.tracker.complete_pipeline_step(
                step_id,
                status="failed",
                error_message=str(e)
            )
            raise
    
    def load(self):
        """Load transformed order data to database"""
        return load_data(self.run_id, self.tracker, self.config)