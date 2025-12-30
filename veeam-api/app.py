"""
API Flask para procesar inventarios de Veeam One
Diseñada para desplegarse en Render.com

Endpoints:
    POST /transform - Recibe 2 archivos Excel y retorna el archivo combinado
    GET /health - Health check
"""

from flask import Flask, request, send_file, jsonify
import pandas as pd
import io
from datetime import datetime
import os

app = Flask(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE PROCESAMIENTO (extraídas de transform_veeam_merge.py)
# ═══════════════════════════════════════════════════════════════════════════════

def extract_host_data_simple(df, host_type, source_file):
    """
    Extrae información básica de hosts (Hyper-V o VMware)
    Solo 4 columnas: host_name, virtualizador, cpu, memoria_ram_gb
    """
    hosts_data = []
    current_host = None
    host_properties = {}
    
    for idx, row in df.iterrows():
        if host_type == 'hyperv':
            host_name = row[3] if len(row) > 3 and pd.notna(row[3]) else None
            property_name = row[6] if len(row) > 6 and pd.notna(row[6]) else None
            property_value = row[8] if len(row) > 8 and pd.notna(row[8]) else None
        else:  # vmware
            host_name = row[2] if len(row) > 2 and pd.notna(row[2]) else None
            property_name = row[5] if len(row) > 5 and pd.notna(row[5]) else None
            property_value = row[7] if len(row) > 7 and pd.notna(row[7]) else None
        
        if host_name and host_name != current_host:
            if current_host and host_properties:
                host_record = {
                    'host_name': current_host,
                    'virtualizador': 'Hyper-V' if host_type == 'hyperv' else 'VMware',
                    **host_properties
                }
                hosts_data.append(host_record)
            current_host = host_name
            host_properties = {}
        
        if property_name and property_value:
            prop_lower = str(property_name).lower()
            
            if 'cpu cores count' in prop_lower or 'cpu threads count' in prop_lower or 'processor cores count' in prop_lower:
                try:
                    host_properties['cpu'] = int(property_value)
                except:
                    host_properties['cpu'] = property_value
            elif 'cpu: sockets count' in prop_lower or 'cpu: packages' in prop_lower or 'processors count' in prop_lower:
                try:
                    sockets = int(property_value)
                    if 'cpu' in host_properties:
                        host_properties['cpu'] = host_properties['cpu'] * sockets
                    else:
                        host_properties['cpu_sockets'] = sockets
                except:
                    pass
            
            if 'memory: size (gb)' in prop_lower:
                try:
                    host_properties['memoria_ram_gb'] = round(float(property_value), 2)
                except:
                    pass
            elif 'memory: size (mb)' in prop_lower and 'memoria_ram_gb' not in host_properties:
                try:
                    memoria_bytes = float(property_value)
                    host_properties['memoria_ram_gb'] = round(memoria_bytes / (1024**3), 2)
                except:
                    pass
            elif 'memory: size (bytes)' in prop_lower and 'memoria_ram_gb' not in host_properties:
                try:
                    host_properties['memoria_ram_gb'] = round(float(property_value) / (1024**3), 2)
                except:
                    pass
    
    if current_host and host_properties:
        host_record = {
            'host_name': current_host,
            'virtualizador': 'Hyper-V' if host_type == 'hyperv' else 'VMware',
            **host_properties
        }
        hosts_data.append(host_record)
    
    df_result = pd.DataFrame(hosts_data)
    
    if len(df_result) == 0:
        return df_result
    
    if 'host_name' in df_result.columns:
        df_result['host_name'] = df_result['host_name'].apply(
            lambda x: x.split('.')[0] if pd.notna(x) and isinstance(x, str) else x
        )
    
    if 'cpu_sockets' in df_result.columns and 'cpu' not in df_result.columns:
        df_result['cpu'] = df_result['cpu_sockets']
    
    final_columns = ['host_name', 'virtualizador', 'cpu', 'memoria_ram_gb']
    for col in final_columns:
        if col not in df_result.columns:
            df_result[col] = None
    
    return df_result[final_columns]


def extract_vm_data_clean(df, vm_type, source_file):
    """
    Extrae solo las columnas esenciales de las VMs
    """
    vms_data = []
    current_vm = None
    current_location = None
    vm_properties = {}
    
    for idx, row in df.iterrows():
        if pd.isna(row[1]) and pd.isna(row[2]):
            continue
            
        location = row[1]
        vm_name = row[2]
        property_name = row[5]
        property_value = row[7]
        
        if not pd.isna(vm_name) and vm_name != current_vm:
            if current_vm is not None:
                vm_record = {
                    'vm_name': current_vm,
                    'virtualization_host': current_location,
                    'source_file': source_file,
                    **vm_properties
                }
                vms_data.append(vm_record)
            
            current_vm = vm_name
            current_location = location if not pd.isna(location) else current_location
            vm_properties = {}
        
        if not pd.isna(property_name) and not pd.isna(property_value):
            prop_lower = str(property_name).lower()
            
            if 'computer name' in prop_lower:
                vm_properties['dns_name'] = property_value
            elif 'ip address' in prop_lower or property_name == 'IP address':
                if 'ip_address' not in vm_properties:
                    vm_properties['ip_address'] = property_value
            elif 'guest os' in prop_lower and 'name' not in prop_lower:
                vm_properties['operating_system'] = property_value
            elif vm_type == 'hyperv' and 'sockets count' in prop_lower:
                vm_properties['cpu_sockets'] = property_value
            elif vm_type == 'hyperv' and 'processors per socket' in prop_lower:
                vm_properties['cpu_per_socket'] = property_value
            elif vm_type == 'vmware' and 'number of cpus' == prop_lower:
                vm_properties['cpu_count'] = property_value
            elif vm_type == 'vmware' and 'vcpu count' == prop_lower:
                vm_properties['vcpu_count'] = property_value
            elif vm_type == 'hyperv' and property_name == 'Memory: Size (MB)':
                vm_properties['memory_mb'] = property_value
            elif vm_type == 'vmware' and 'memory: amount' in prop_lower:
                vm_properties['memory_mb'] = property_value
            elif 'virtual disk: size total' in prop_lower:
                try:
                    disk_bytes = float(property_value)
                    disk_gb = disk_bytes / (1024**3)
                    vm_properties['disk_total_gb'] = round(disk_gb, 2)
                except:
                    vm_properties['disk_total_gb'] = property_value
            elif vm_type == 'vmware' and property_name == 'Storage':
                try:
                    storage_bytes = float(property_value)
                    storage_gb = storage_bytes / (1024**3)
                    vm_properties['storage_used_gb'] = round(storage_gb, 2)
                except:
                    pass
            elif vm_type == 'vmware' and 'has snapshots' in prop_lower:
                vm_properties['has_snapshots'] = property_value
            elif vm_type == 'hyperv' and 'recent snapshots' in str(property_value).lower():
                vm_properties['has_snapshots'] = 'Yes'
            elif vm_type == 'hyperv' and 'no snapshots' in str(property_value).lower():
                vm_properties['has_snapshots'] = 'No'
            elif 'power state' in prop_lower:
                vm_properties['power_state'] = property_value
            elif vm_type == 'vmware' and 'host system' == prop_lower:
                vm_properties['host_system'] = property_value
    
    if current_vm is not None:
        vm_record = {
            'vm_name': current_vm,
            'virtualization_host': current_location,
            'source_file': source_file,
            **vm_properties
        }
        vms_data.append(vm_record)
    
    df_result = pd.DataFrame(vms_data)
    
    if vm_type == 'hyperv':
        if 'cpu_sockets' in df_result.columns and 'cpu_per_socket' in df_result.columns:
            df_result['cpu_sockets'] = pd.to_numeric(df_result['cpu_sockets'], errors='coerce')
            df_result['cpu_per_socket'] = pd.to_numeric(df_result['cpu_per_socket'], errors='coerce')
            df_result['cpu_count'] = df_result['cpu_sockets'] * df_result['cpu_per_socket']
            df_result['cpu_count'] = df_result['cpu_count'].fillna(df_result['cpu_sockets'])
        
        if 'virtualization_host' in df_result.columns:
            df_result['virtualization_host'] = df_result['virtualization_host'].apply(
                lambda x: x.split('.')[0] if pd.notna(x) else x
            )
    
    elif vm_type == 'vmware':
        if 'host_system' in df_result.columns:
            df_result['virtualization_host'] = df_result['host_system']
        
        if 'virtualization_host' in df_result.columns:
            df_result['virtualization_host'] = df_result['virtualization_host'].apply(
                lambda x: x.split('>')[-1].split('.')[0] if pd.notna(x) and '>' in str(x) else 
                         (x.split('.')[0] if pd.notna(x) else x)
            )
        
        if 'vcpu_count' in df_result.columns and 'cpu_count' not in df_result.columns:
            df_result['cpu_count'] = df_result['vcpu_count']
    
    if 'memory_mb' in df_result.columns:
        df_result['memory_mb'] = pd.to_numeric(df_result['memory_mb'], errors='coerce')
        df_result['memory_gb'] = (df_result['memory_mb'] / 1024).round(2)
    
    final_columns = [
        'vm_name', 'virtualization_host', 'source_file', 'dns_name',
        'ip_address', 'operating_system', 'cpu_count', 'memory_gb',
        'disk_total_gb', 'has_snapshots', 'power_state'
    ]
    
    available_columns = [col for col in final_columns if col in df_result.columns]
    df_result = df_result[available_columns]
    
    if 'has_snapshots' in df_result.columns:
        df_result['has_snapshots'] = df_result['has_snapshots'].fillna('Unknown')
    
    if 'vm_name' in df_result.columns:
        df_result['is_replica_or_crd'] = df_result['vm_name'].apply(
            lambda x: 'Yes' if pd.notna(x) and (
                'replica' in str(x).lower() or 
                'crd' in str(x).lower()
            ) else 'No'
        )
    
    return df_result


def process_excel_file(file_stream, filename):
    """
    Procesa un archivo Excel de inventario Veeam
    Retorna DataFrames de VMs y Hosts
    """
    xl_file = pd.ExcelFile(file_stream)
    
    vms_list = []
    hosts_list = []
    
    # Procesar VMs
    sheets_to_process = []
    if 'Sheet6' in xl_file.sheet_names:
        sheets_to_process.append(('Sheet6', 'hyperv'))
    if 'Sheet33' in xl_file.sheet_names:
        sheets_to_process.append(('Sheet33', 'vmware'))
    
    # Procesar Hosts
    host_sheets = []
    if 'Sheet1' in xl_file.sheet_names:
        host_sheets.append(('Sheet1', 'hyperv'))
    if 'Sheet22' in xl_file.sheet_names:
        host_sheets.append(('Sheet22', 'vmware'))
    
    for sheet_name, vm_type in sheets_to_process:
        df_raw = pd.read_excel(file_stream, sheet_name=sheet_name, header=None)
        df_clean = extract_vm_data_clean(df_raw, vm_type, filename)
        df_clean.insert(1, 'vm_type', 'Hyper-V' if vm_type == 'hyperv' else 'VMware')
        vms_list.append(df_clean)
    
    for sheet_name, host_type in host_sheets:
        df_raw = pd.read_excel(file_stream, sheet_name=sheet_name, header=None)
        df_hosts = extract_host_data_simple(df_raw, host_type, filename)
        if len(df_hosts) > 0:
            hosts_list.append(df_hosts)
    
    return vms_list, hosts_list


def combine_inventories(files_data):
    """
    Combina múltiples inventarios en un solo archivo
    files_data: lista de tuplas (file_stream, filename)
    """
    all_vms = []
    all_hosts = []
    
    for file_stream, filename in files_data:
        vms_list, hosts_list = process_excel_file(file_stream, filename)
        all_vms.extend(vms_list)
        all_hosts.extend(hosts_list)
    
    # Combinar VMs
    df_combined = None
    if all_vms:
        df_combined = pd.concat(all_vms, axis=0, ignore_index=True, sort=False)
        # Eliminar headers
        df_combined = df_combined[~df_combined['vm_name'].str.contains('Virtual Machine Name', case=False, na=False)]
        # Eliminar duplicados
        df_combined = df_combined.drop_duplicates(subset=['vm_name'], keep='first')
    
    # Combinar Hosts
    df_hosts_combined = None
    if all_hosts:
        df_hosts_combined = pd.concat(all_hosts, axis=0, ignore_index=True, sort=False)
        df_hosts_combined = df_hosts_combined.drop_duplicates(subset=['host_name'], keep='first')
    
    return df_combined, df_hosts_combined


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS DE LA API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'Veeam Inventory Transformer',
        'timestamp': datetime.now().isoformat()
    })


@app.route('/transform', methods=['POST'])
def transform_inventories():
    """
    Endpoint principal para transformar inventarios
    
    Espera recibir archivos via multipart/form-data:
    - file1: Primer archivo Excel (ej: Inventory triara)
    - file2: Segundo archivo Excel (ej: Inventory)
    
    Retorna: Archivo Excel combinado
    """
    # Validar que se recibieron archivos
    if 'file1' not in request.files or 'file2' not in request.files:
        return jsonify({
            'error': 'Se requieren 2 archivos: file1 y file2',
            'usage': 'POST /transform con multipart/form-data conteniendo file1 y file2'
        }), 400
    
    file1 = request.files['file1']
    file2 = request.files['file2']
    
    # Validar que los archivos tienen nombre
    if file1.filename == '' or file2.filename == '':
        return jsonify({'error': 'Los archivos deben tener nombre'}), 400
    
    try:
        # Preparar los archivos para procesamiento
        files_data = [
            (io.BytesIO(file1.read()), file1.filename),
            (io.BytesIO(file2.read()), file2.filename)
        ]
        
        # Procesar y combinar
        df_vms, df_hosts = combine_inventories(files_data)
        
        if df_vms is None and df_hosts is None:
            return jsonify({'error': 'No se pudieron procesar los archivos'}), 400
        
        # Crear archivo Excel de salida en memoria
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if df_vms is not None and len(df_vms) > 0:
                df_vms.to_excel(writer, sheet_name='Consolidado_VMs', index=False)
            if df_hosts is not None and len(df_hosts) > 0:
                df_hosts.to_excel(writer, sheet_name='Consolidado_Hosts', index=False)
        
        output.seek(0)
        
        # Generar nombre de archivo con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"Inventory_Merged_{timestamp}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=output_filename
        )
    
    except Exception as e:
        return jsonify({
            'error': f'Error procesando archivos: {str(e)}'
        }), 500


@app.route('/', methods=['GET'])
def index():
    """Página principal con información de la API"""
    return jsonify({
        'service': 'Veeam Inventory Transformer API',
        'version': '1.0.0',
        'endpoints': {
            'GET /': 'Esta información',
            'GET /health': 'Health check',
            'POST /transform': 'Combinar 2 archivos de inventario Veeam'
        },
        'usage': {
            'transform': {
                'method': 'POST',
                'content_type': 'multipart/form-data',
                'fields': {
                    'file1': 'Primer archivo Excel (ej: Inventory_triara_YYYY_MM_DD.xlsx)',
                    'file2': 'Segundo archivo Excel (ej: Inventory_YYYY_MM_DD.xlsx)'
                },
                'response': 'Archivo Excel combinado (Inventory_Merged_TIMESTAMP.xlsx)'
            }
        }
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)