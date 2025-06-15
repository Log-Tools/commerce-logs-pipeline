#!/usr/bin/env python3
"""
Explore Raw Logs CLI Tool

This tool allows you to consume and explore raw logs from the Ingestion.RawLogs Kafka topic.
It provides filtering, searching, and display capabilities for debugging and analysis.
"""

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Pattern, Any

try:
    from pykafka import KafkaClient
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    import yaml
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("💡 Install with: pip install pykafka rich pyyaml")
    sys.exit(1)


def safe_eval_filter(expression: str, record: Dict) -> bool:
    """Safely evaluates a Python expression against a record"""
    try:
        # Create a safe namespace with the record and common functions
        safe_namespace = {
            'record': record,
            'r': record,  # Short alias
            'len': len,
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'isinstance': isinstance,
            'hasattr': hasattr,
            'getattr': getattr,
            'get': lambda obj, key, default=None: obj.get(key, default) if hasattr(obj, 'get') else default,
            'contains': lambda container, item: item in container if container else False,
            'startswith': lambda s, prefix: str(s).startswith(prefix) if s else False,
            'endswith': lambda s, suffix: str(s).endswith(suffix) if s else False,
            'lower': lambda s: str(s).lower() if s else '',
            'upper': lambda s: str(s).upper() if s else '',
            'in_range': lambda val, min_val, max_val: min_val <= val <= max_val,
            're': re,  # For regex matching
        }
        
        # Remove dangerous built-ins
        safe_namespace['__builtins__'] = {}
        
        result = eval(expression, safe_namespace)
        return bool(result)
    except Exception as e:
        # If evaluation fails, return False and optionally log
        return False


def extract_nested_value(obj: Any, path: str, default=None) -> Any:
    """Extracts a value from nested dict using dot notation (e.g., 'kubernetes.pod_name')"""
    try:
        parts = path.split('.')
        current = obj
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return default
            if current is None:
                return default
        return current
    except:
        return default


def detect_common_fields(messages: List[Dict]) -> List[Dict]:
    """Analyzes messages to detect common fields for dynamic table display"""
    if not messages:
        return []
    
    # Count field occurrences across messages
    field_counts = defaultdict(int)
    sample_values = defaultdict(set)
    
    for msg in messages[:10]:  # Sample first 10 messages
        parsed = msg.get('parsed_value', {})
        if isinstance(parsed, dict):
            # Check top-level fields
            for key, value in parsed.items():
                field_counts[key] += 1
                if value is not None:
                    sample_values[key].add(str(value)[:20])  # Sample values for type detection
            
            # Check common nested paths
            nested_paths = [
                'kubernetes.pod_name',
                'kubernetes.container_name', 
                'kubernetes.namespace_name',
                'logs.status',
                'logs.requestFirstLine',
                'logs.userAgent',
                'logs.remoteHost',
                'logs.responseTime',
                'record_date',
                'stream',
                '@timestamp'
            ]
            
            for path in nested_paths:
                value = extract_nested_value(parsed, path)
                if value is not None:
                    field_counts[f'nested.{path}'] += 1
                    sample_values[f'nested.{path}'].add(str(value)[:20])
    
    # Rank fields by frequency and usefulness
    common_fields = []
    
    # Always include basic fields
    common_fields.extend([
        {'name': 'partition', 'path': 'partition', 'width': 8},
        {'name': 'offset', 'path': 'offset', 'width': 10},
        {'name': 'timestamp', 'path': 'timestamp', 'width': 20},
    ])
    
    # Add metadata fields if available
    if field_counts.get('subscription', 0) > 0:
        common_fields.append({'name': 'subscription', 'path': 'subscription', 'width': 12})
    if field_counts.get('environment', 0) > 0:
        common_fields.append({'name': 'environment', 'path': 'environment', 'width': 10})
    
    # Add the most common nested fields
    high_frequency_nested = [
        ('nested.kubernetes.pod_name', 'pod', 20),
        ('nested.kubernetes.container_name', 'container', 15),
        ('nested.logs.status', 'status', 8),
        ('nested.logs.requestFirstLine', 'request', 30),
        ('nested.record_date', 'date', 12),
        ('nested.stream', 'stream', 10),
    ]
    
    for field_key, display_name, width in high_frequency_nested:
        if field_counts.get(field_key, 0) > len(messages) * 0.3:  # Present in >30% of messages
            path = field_key.replace('nested.', '')
            common_fields.append({'name': display_name, 'path': path, 'width': width})
    
    # Add a preview field for remaining content
    common_fields.append({'name': 'preview', 'path': 'value', 'width': 50})
    
    return common_fields


class RawLogsExplorer:
    """Explores raw logs from Kafka topic with filtering and display capabilities"""
    
    def __init__(self, kafka_brokers: str, topic: str = "Ingestion.RawLogs"):
        self.kafka_brokers = kafka_brokers
        self.topic = topic
        self.console = Console()
        self.client = None
        
    def connect(self) -> bool:
        """Establishes connection to Kafka"""
        try:
            # Create Kafka client
            self.client = KafkaClient(hosts=self.kafka_brokers)
            
            # Test connection by getting topics
            topics = self.client.topics
            self.console.print(f"✅ Connected to Kafka at {self.kafka_brokers}")
            
            # Check if topic exists
            if self.topic.encode('utf-8') not in topics:
                self.console.print(f"⚠️ Warning: Topic '{self.topic}' not found in cluster")
                self.console.print(f"Available topics: {[t.decode('utf-8') for t in topics.keys()]}")
            
            return True
            
        except Exception as e:
            self.console.print(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def get_topic_info(self) -> Dict:
        """Gets information about the topic"""
        try:
            if not hasattr(self, 'client'):
                return {}
                
            # Get topic
            if self.topic.encode('utf-8') not in self.client.topics:
                self.console.print(f"❌ Topic '{self.topic}' not found")
                return {}
            
            topic = self.client.topics[self.topic.encode('utf-8')]
            partitions = list(topic.partitions.keys())
            
            topic_info = {
                'topic': self.topic,
                'partitions': partitions,
                'partition_info': {}
            }
            
            # Get offset information for each partition
            for partition_id in partitions:
                try:
                    partition = topic.partitions[partition_id]
                    # Get earliest and latest offsets using pykafka API
                    earliest_offsets = partition.earliest_available_offset()
                    latest_offsets = partition.latest_available_offset()
                    
                    topic_info['partition_info'][partition_id] = {
                        'earliest_offset': earliest_offsets,
                        'latest_offset': latest_offsets,
                        'message_count': latest_offsets - earliest_offsets
                    }
                except Exception as e:
                    self.console.print(f"⚠️ Could not get offsets for partition {partition_id}: {e}")
                    topic_info['partition_info'][partition_id] = {
                        'earliest_offset': 0,
                        'latest_offset': 0,
                        'message_count': 0
                    }
            
            return topic_info
            
        except Exception as e:
            self.console.print(f"❌ Failed to get topic info: {e}")
            return {}
    
    def display_topic_info(self, topic_info: Dict):
        """Displays topic information in a formatted table"""
        if not topic_info:
            return
            
        table = Table(title=f"Topic Information: {topic_info['topic']}")
        table.add_column("Partition", style="cyan")
        table.add_column("Earliest Offset", style="green")
        table.add_column("Latest Offset", style="yellow")
        table.add_column("Message Count", style="magenta")
        
        total_messages = 0
        for partition in topic_info['partitions']:
            info = topic_info['partition_info'][partition]
            table.add_row(
                str(partition),
                str(info['earliest_offset']),
                str(info['latest_offset']),
                str(info['message_count'])
            )
            total_messages += info['message_count']
        
        table.add_row("TOTAL", "", "", str(total_messages), style="bold")
        self.console.print(table)
    
    def consume_logs(self, 
                    max_messages: int = 100,
                    from_beginning: bool = True,
                    filter_pattern: Optional[str] = None,
                    object_filter: Optional[str] = None,
                    subscription_filter: Optional[str] = None,
                    environment_filter: Optional[str] = None,
                    blob_filter: Optional[str] = None,
                    output_format: str = "table",
                    skip_matched: int = 0) -> List[Dict]:
        """Consumes raw logs from the topic with optional filtering"""
        
        if not self.client:
            self.console.print("❌ Not connected to Kafka")
            return []
        
        try:
            # Get topic and create consumer
            topic = self.client.topics[self.topic.encode('utf-8')]
            consumer = topic.get_simple_consumer(
                consumer_group=f'explore-raw-logs-{int(time.time())}'.encode('utf-8'),
                auto_offset_reset=0 if from_beginning else -1,  # 0 = earliest, -1 = latest
                reset_offset_on_start=from_beginning,
                consumer_timeout_ms=10000  # 10 second timeout
            )
            
            # If from_beginning, we need to seek to beginning after assignment
            if from_beginning:
                self.console.print("🔄 Seeking to beginning of topic...")
            else:
                self.console.print("🔄 Seeking to latest messages...")
        
            messages = []
            pattern = re.compile(filter_pattern, re.IGNORECASE) if filter_pattern else None
            timeout_count = 0
            max_timeouts = 3  # Reduce timeouts since we have a longer consumer timeout
            
            # Counters for skip and total
            total_matched = 0
            skipped_count = 0
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console
            ) as progress:
                task = progress.add_task("Consuming messages...", total=max_messages)
                
                while len(messages) < max_messages and timeout_count < max_timeouts:
                    try:
                        msg = consumer.consume(block=False)  # Non-blocking to handle timeouts better
                        
                        if msg is None:
                            timeout_count += 1
                            time.sleep(1)  # Wait a bit before retrying
                            continue
                            
                        timeout_count = 0  # Reset timeout counter
                        
                        # Parse message
                        try:
                            msg_data = {
                                'partition': msg.partition_id,
                                'offset': msg.offset,
                                'key': msg.partition_key.decode('utf-8') if msg.partition_key else None,
                                'value': msg.value.decode('utf-8') if msg.value else None,
                                'timestamp': datetime.fromtimestamp(msg.timestamp / 1000) if hasattr(msg, 'timestamp') and msg.timestamp and msg.timestamp > 0 else None
                            }
                            
                            # Parse key for metadata
                            if msg_data['key']:
                                key_parts = msg_data['key'].split(':')
                                if len(key_parts) >= 2:
                                    msg_data['subscription'] = key_parts[0]
                                    msg_data['environment'] = key_parts[1]
                                    if len(key_parts) > 3:
                                        # Extract blob name from key (last part)
                                        msg_data['blob_name'] = key_parts[-1]
                            
                            # Try to parse JSON value
                            try:
                                if msg_data['value']:
                                    parsed_json = json.loads(msg_data['value'])
                                    msg_data['parsed_value'] = parsed_json
                            except json.JSONDecodeError:
                                # Not JSON, keep as plain text
                                msg_data['parsed_value'] = msg_data['value']
                            
                            # Apply filters
                            if subscription_filter and msg_data.get('subscription') != subscription_filter:
                                continue
                            if environment_filter and msg_data.get('environment') != environment_filter:
                                continue
                            if blob_filter and blob_filter not in msg_data.get('blob_name', ''):
                                continue
                            if pattern and not pattern.search(msg_data['value'] or ''):
                                continue
                            
                            # Apply object filter (Python expression) - now includes key access
                            if object_filter:
                                if not safe_eval_filter(object_filter, msg_data):
                                    continue
                            
                            # Count this as a match
                            total_matched += 1
                            
                            # Skip if we haven't reached the skip threshold
                            if skipped_count < skip_matched:
                                skipped_count += 1
                                continue
                            
                            messages.append(msg_data)
                            progress.update(task, advance=1)
                            
                        except Exception as e:
                            self.console.print(f"⚠️ Error processing message: {e}")
                            continue
                            
                    except Exception as e:
                        # Handle timeout or other consumer errors
                        timeout_count += 1
                        continue
                        
                if timeout_count >= max_timeouts:
                    self.console.print(f"⏰ Stopped after {max_timeouts} consecutive timeouts")
                    
            # Close consumer
            consumer.stop()
            
            # Display summary
            self.console.print(f"\n[bold green]📊 Summary:[/bold green]")
            self.console.print(f"  Total matched: {total_matched}")
            if skip_matched > 0:
                self.console.print(f"  Skipped: {skip_matched}")
            self.console.print(f"  Displayed: {len(messages)}")
        
        except KeyboardInterrupt:
            self.console.print("\n⚠️ Consumption interrupted by user")
        except Exception as e:
            self.console.print(f"❌ Error consuming messages: {e}")
        
        self.display_messages(messages, output_format)
        return messages
    
    def display_messages(self, messages: List[Dict], output_format: str):
        """Displays consumed messages in the specified format"""
        if not messages:
            self.console.print("📭 No messages found matching the criteria")
            return
        
        if output_format == "table":
            self._display_table(messages)
        elif output_format == "json":
            self._display_json(messages)
        elif output_format == "raw":
            self._display_raw(messages)
        else:
            self.console.print(f"❌ Unknown output format: {output_format}")
    
    def _display_table(self, messages: List[Dict]):
        """Displays messages showing the exact raw content from Kafka"""
        if not messages:
            return
            
        self.console.print(f"\n[bold green]📋 Found {len(messages)} messages from {self.topic}[/bold green]\n")
        
        for i, msg in enumerate(messages[:20], 1):
            # Header with basic Kafka metadata only
            offset = msg.get('offset', 'N/A')
            partition = msg.get('partition', 'N/A')
            key = msg.get('key', 'N/A')
            
            # Show basic Kafka info
            self.console.print(f"[bold cyan]Message #{i}[/bold cyan]")
            self.console.print(f"  [dim]Offset: {offset} | Partition: {partition}[/dim]")
            if key and key != 'N/A':
                # Display key without rich formatting to avoid random coloring
                print(f"  Key: {key}")
            
            # Show the EXACT raw value from Kafka - no assumptions!
            raw_value = msg.get('value', '')
            self.console.print(f"  [yellow]Raw Value:[/yellow]")
            
            # Pretty print JSON with syntax highlighting
            try:
                if raw_value:
                    parsed_json = json.loads(raw_value)
                    # Use rich's JSON printing for proper syntax highlighting
                    self.console.print_json(json.dumps(parsed_json, ensure_ascii=False))
                else:
                    self.console.print("(empty)")
            except json.JSONDecodeError:
                # Not JSON, show exactly as-is
                self.console.print(raw_value)
            
            # Separator between messages
            if i < min(len(messages), 20):
                self.console.print("\n" + "─" * 80 + "\n")
        
        if len(messages) > 20:
            self.console.print(f"[dim]... and {len(messages) - 20} more messages[/dim]")
            self.console.print(f"[dim]💡 Use --max-messages to control how many to show[/dim]")
    
    def _display_json(self, messages: List[Dict]):
        """Displays messages in JSON format"""
        for msg in messages:
            # Convert timestamp to string for JSON serialization
            display_msg = msg.copy()
            if display_msg['timestamp']:
                display_msg['timestamp'] = display_msg['timestamp'].isoformat()
            self.console.print_json(json.dumps(display_msg, indent=2))
    
    def _display_raw(self, messages: List[Dict]):
        """Displays raw log lines only"""
        for msg in messages:
            self.console.print(msg.get('value', ''))
    
    def analyze_logs(self, messages: List[Dict]) -> Dict:
        """Analyzes the consumed log messages for patterns and statistics"""
        if not messages:
            return {}
        
        analysis = {
            'total_messages': len(messages),
            'subscriptions': defaultdict(int),
            'environments': defaultdict(int),
            'blobs': defaultdict(int),
            'partitions': defaultdict(int),
            'data_types': defaultdict(int),
            'top_level_fields': defaultdict(int),
            'timestamps': []
        }
        
        for msg in messages:
            if msg.get('subscription'):
                analysis['subscriptions'][msg['subscription']] += 1
            if msg.get('environment'):
                analysis['environments'][msg['environment']] += 1
            if msg.get('blob_name'):
                analysis['blobs'][msg['blob_name']] += 1
                
            analysis['partitions'][msg['partition']] += 1
            
            if msg['timestamp']:
                analysis['timestamps'].append(msg['timestamp'])
            
            # Analyze data structure
            parsed = msg.get('parsed_value')
            if isinstance(parsed, dict):
                analysis['data_types']['json_object'] += 1
                for key in parsed.keys():
                    analysis['top_level_fields'][key] += 1
            elif isinstance(parsed, str):
                analysis['data_types']['text'] += 1
            else:
                analysis['data_types']['other'] += 1
        
        return analysis
    
    def display_analysis(self, analysis: Dict):
        """Displays log analysis results"""
        if not analysis:
            return
        
        # Summary panel
        summary = Panel(
            f"Total Messages: {analysis['total_messages']}\n"
            f"Unique Subscriptions: {len(analysis['subscriptions'])}\n"
            f"Unique Environments: {len(analysis['environments'])}\n"
            f"Unique Blobs: {len(analysis['blobs'])}\n"
            f"Partitions Used: {len(analysis['partitions'])}",
            title="Log Analysis Summary",
            border_style="green"
        )
        self.console.print(summary)
        
        # Display analysis tables
        tables = [
            ("Top Subscriptions", analysis['subscriptions'], "cyan"),
            ("Environments", analysis['environments'], "yellow"),
            ("Data Types", analysis['data_types'], "green"),
            ("Common JSON Fields", analysis['top_level_fields'], "magenta"),
            ("Top Blobs", analysis['blobs'], "white")
        ]
        
        for title, data, color in tables:
            if data:
                table = Table(title=title)
                table.add_column("Item", style=color)
                table.add_column("Count", style="green")
                for item, count in sorted(data.items(), key=lambda x: x[1], reverse=True)[:10]:
                    table.add_row(str(item), str(count))
                self.console.print(table)
    
    def close(self):
        """Closes the Kafka consumer connection"""
        if self.client:
            # pykafka KafkaClient doesn't need explicit closing
            self.console.print("👋 Kafka consumer closed")


def load_config() -> Dict:
    """Loads configuration from config files"""
    config_paths = [
        Path.cwd() / "config" / "kafka.yaml",
        Path.cwd() / "configs" / "kafka_topics.yaml",
        Path(__file__).parent.parent / "config" / "kafka.yaml"
    ]
    
    for config_path in config_paths:
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f)
            except Exception as e:
                print(f"Warning: Failed to load config from {config_path}: {e}")
    
    return {}


def main():
    parser = argparse.ArgumentParser(
        description="Explore raw logs from the Ingestion.RawLogs Kafka topic",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Filter Examples:

String-based filtering (--filter-pattern):
  # Find logs containing text
  explore-raw-logs --filter-pattern "GET|POST"
  explore-raw-logs --filter-pattern '"status":"500"'

Object-based filtering (--object-filter):
  # Python expressions on parsed JSON
  explore-raw-logs --object-filter "r['parsed_value'].get('logs', {}).get('status') == '500'"
  explore-raw-logs --object-filter "get(r['parsed_value'], 'kubernetes', {}).get('pod_name', '').startswith('apache')"
  explore-raw-logs --object-filter "r['parsed_value'].get('@timestamp', '').startswith('2025-06-09')"
  
  # Filter by Kafka key
  explore-raw-logs --object-filter "'apache2-igc' not in r.get('key', '')"
  explore-raw-logs --object-filter "r.get('key', '').startswith('cp2:D1')"
  explore-raw-logs --object-filter "'proxy' in r.get('key', '')"
  
  # Complex conditions
  explore-raw-logs --object-filter "isinstance(r['parsed_value'], dict) and 'logs' in r['parsed_value']"
  explore-raw-logs --object-filter "int(get(r['parsed_value'], 'logs', {}).get('status', 0)) >= 400"

Available variables in object filters:
  - r or record: the full message record
  - r['key']: Kafka message key
  - r['parsed_value']: parsed JSON content
  - r['value'], r['offset'], r['partition'], etc.
  
Helper functions:
  - get(obj, key, default): safe dict access
  - contains(container, item): safe membership test
  - startswith(s, prefix), endswith(s, suffix): string operations
  - int(), str(), len(), isinstance(): type operations

Examples:
  # Get last 10 stderr messages
  explore-raw-logs --from-latest --object-filter "r['parsed_value'].get('stream') == 'stderr'" --max-messages 10
  
  # Skip first 5 matches, show next 10
  explore-raw-logs --object-filter "r['parsed_value'].get('stream') == 'stderr'" --skip 5 --max-messages 10
  
  # Find messages NOT from apache pods
  explore-raw-logs --object-filter "'apache2-igc' not in r.get('key', '')" --max-messages 5
        """
    )
    
    parser.add_argument("--kafka-brokers", default="localhost:9092",
                       help="Kafka brokers (default: localhost:9092)")
    parser.add_argument("--topic", default="Ingestion.RawLogs",
                       help="Kafka topic to consume from (default: Ingestion.RawLogs)")
    parser.add_argument("--max-messages", type=int, default=100,
                       help="Maximum number of messages to consume (default: 100)")
    parser.add_argument("--from-beginning", action="store_true", default=True,
                       help="Start consuming from the beginning of the topic")
    parser.add_argument("--from-latest", action="store_true",
                       help="Start consuming from the latest messages")
    parser.add_argument("--filter-pattern", type=str,
                       help="Regex pattern to filter log content (treats JSON as string)")
    parser.add_argument("--object-filter", type=str,
                       help="Python expression to filter parsed objects (e.g., \"r['parsed_value'].get('logs', {}).get('status') == '500'\")")
    parser.add_argument("--subscription", type=str,
                       help="Filter by subscription ID")
    parser.add_argument("--environment", type=str,
                       help="Filter by environment")
    parser.add_argument("--blob-filter", type=str,
                       help="Filter by blob name (partial match)")
    parser.add_argument("--output-format", choices=["table", "json", "raw"], default="table",
                       help="Output format (default: table)")
    parser.add_argument("--skip", type=int, default=0,
                       help="Skip the first N matched messages (default: 0)")
    parser.add_argument("--info", action="store_true",
                       help="Show topic information only")
    parser.add_argument("--analyze", action="store_true",
                       help="Analyze consumed messages for patterns")
    
    args = parser.parse_args()
    
    # Override from_beginning if from_latest is specified
    if args.from_latest:
        args.from_beginning = False
    
    # Load configuration
    config = load_config()
    kafka_brokers = args.kafka_brokers
    if config.get('kafka', {}).get('brokers'):
        kafka_brokers = config['kafka']['brokers']
    
    # Create explorer
    explorer = RawLogsExplorer(kafka_brokers, args.topic)
    
    try:
        # Connect to Kafka
        if not explorer.connect():
            sys.exit(1)
        
        # Show topic info if requested
        if args.info:
            topic_info = explorer.get_topic_info()
            explorer.display_topic_info(topic_info)
            return
        
        # Consume messages
        messages = explorer.consume_logs(
            max_messages=args.max_messages,
            from_beginning=args.from_beginning,
            filter_pattern=args.filter_pattern,
            object_filter=args.object_filter,
            subscription_filter=args.subscription,
            environment_filter=args.environment,
            blob_filter=args.blob_filter,
            output_format=args.output_format,
            skip_matched=args.skip
        )
        
        # Analyze if requested
        if args.analyze and messages:
            analysis = explorer.analyze_logs(messages)
            explorer.display_analysis(analysis)
            
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        explorer.close()


if __name__ == "__main__":
    main() 