# Test Framework for AI Functions
# Python utilities for testing AI Functions in Databricks

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from typing import List, Dict, Any
import time

class AIFunctionTestFramework:
    """Framework for testing AI Functions with assertions and benchmarking"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.test_results = []
    
    def test_classification(
        self,
        function_name: str,
        test_cases: List[Dict[str, Any]],
        model: str = 'databricks-meta-llama-3-1-70b-instruct'
    ) -> Dict[str, Any]:
        """
        Test classification function with multiple test cases
        
        Args:
            function_name: Name of the function to test
            test_cases: List of test cases with input and expected output
            model: Model endpoint to use
            
        Returns:
            Dictionary with test results
        """
        results = {
            'function': function_name,
            'total_tests': len(test_cases),
            'passed': 0,
            'failed': 0,
            'avg_latency_ms': 0,
            'details': []
        }
        
        total_latency = 0
        
        for test_case in test_cases:
            start_time = time.time()
            
            try:
                # Execute classification
                df = self.spark.createDataFrame([(test_case['input'],)], ['text'])
                result_df = df.selectExpr(
                    f"AI_CLASSIFY('{model}', text, ARRAY({test_case['categories']})) as result"
                )
                result = result_df.collect()[0]['result']
                
                end_time = time.time()
                latency_ms = (end_time - start_time) * 1000
                total_latency += latency_ms
                
                # Check result
                passed = result == test_case['expected']
                if passed:
                    results['passed'] += 1
                else:
                    results['failed'] += 1
                
                results['details'].append({
                    'test_name': test_case.get('name', 'unnamed'),
                    'passed': passed,
                    'expected': test_case['expected'],
                    'actual': result,
                    'latency_ms': latency_ms
                })
                
            except Exception as e:
                results['failed'] += 1
                results['details'].append({
                    'test_name': test_case.get('name', 'unnamed'),
                    'passed': False,
                    'error': str(e)
                })
        
        results['avg_latency_ms'] = total_latency / len(test_cases)
        return results
    
    def benchmark_function(
        self,
        function_sql: str,
        num_iterations: int = 10,
        input_sizes: List[int] = [100, 500, 1000]
    ) -> Dict[str, Any]:
        """
        Benchmark AI function performance across different input sizes
        
        Args:
            function_sql: SQL expression for the function
            num_iterations: Number of iterations per input size
            input_sizes: List of input string lengths to test
            
        Returns:
            Dictionary with benchmark results
        """
        benchmarks = []
        
        for size in input_sizes:
            test_text = 'a' * size
            latencies = []
            
            for _ in range(num_iterations):
                start_time = time.time()
                
                df = self.spark.createDataFrame([(test_text,)], ['text'])
                df.selectExpr(function_sql).collect()
                
                end_time = time.time()
                latencies.append((end_time - start_time) * 1000)
            
            benchmarks.append({
                'input_size': size,
                'avg_latency_ms': sum(latencies) / len(latencies),
                'min_latency_ms': min(latencies),
                'max_latency_ms': max(latencies),
                'p95_latency_ms': sorted(latencies)[int(len(latencies) * 0.95)]
            })
        
        return {
            'function': function_sql,
            'iterations': num_iterations,
            'benchmarks': benchmarks
        }
    
    def validate_output_format(
        self,
        output: Any,
        expected_type: str,
        schema: Dict[str, Any] = None
    ) -> bool:
        """
        Validate AI function output matches expected format
        
        Args:
            output: The output to validate
            expected_type: Expected type ('string', 'array', 'struct')
            schema: Expected schema for complex types
            
        Returns:
            Boolean indicating if validation passed
        """
        if expected_type == 'string':
            return isinstance(output, str) and len(output) > 0
        elif expected_type == 'array':
            return isinstance(output, list)
        elif expected_type == 'struct':
            return isinstance(output, dict) and (schema is None or all(k in output for k in schema.keys()))
        return False
    
    def generate_test_report(self) -> str:
        """Generate markdown test report"""
        report = ["# AI Function Test Report\n"]
        report.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        for result in self.test_results:
            report.append(f"## {result['function']}\n")
            report.append(f"- Total Tests: {result['total_tests']}\n")
            report.append(f"- Passed: {result['passed']}\n")
            report.append(f"- Failed: {result['failed']}\n")
            report.append(f"- Success Rate: {100 * result['passed'] / result['total_tests']:.1f}%\n")
            report.append(f"- Avg Latency: {result['avg_latency_ms']:.2f}ms\n\n")
        
        return ''.join(report)

# Example usage:
# spark = SparkSession.builder.appName("AIFunctionTesting").getOrCreate()
# framework = AIFunctionTestFramework(spark)
# 
# test_cases = [
#     {'name': 'positive', 'input': 'Great product!', 'categories': "'positive','negative'", 'expected': 'positive'},
#     {'name': 'negative', 'input': 'Terrible service!', 'categories': "'positive','negative'", 'expected': 'negative'}
# ]
# 
# results = framework.test_classification('AI_CLASSIFY', test_cases)
# print(framework.generate_test_report())
