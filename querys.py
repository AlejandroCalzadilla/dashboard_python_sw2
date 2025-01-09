ventas_totales_por_mes_query = """
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        SUM(total_price) AS total_sales
    FROM 
        orders
    WHERE 
        status = 'entregado'
    GROUP BY 
        DATE_TRUNC('month', order_date)
    ORDER BY 
        month;
    """


cantidad_prendas_por_tipo_query= """
    SELECT 
        g.name AS garment_type,
        SUM((oi->>'quantity')::INTEGER) AS total_quantity
    FROM 
        orders o,
        JSONB_ARRAY_ELEMENTS(order_items) oi
    JOIN 
        garments g ON g.id = (oi->>'garmentId')
    GROUP BY 
        g.name
    ORDER BY 
        total_quantity DESC
    LIMIT 5;
    """    


estados_pedidos_actuales_query="""
    SELECT 
        status, 
        COUNT(*) AS total_orders
    FROM 
        orders
    GROUP BY 
        status;
    """    

materiales_mas_utilizados_query="""
    SELECT 
        rm.name AS material_name,
        SUM((dn->>'quantity')::INTEGER) AS total_used
    FROM 
        notes n,
        JSONB_ARRAY_ELEMENTS(detail_notes) dn
    JOIN 
        raw_materials rm ON rm.id = (dn->>'rawMaterialId')
    GROUP BY 
        rm.name
    ORDER BY 
        total_used DESC;
    """    
pedidos_por_cliente_query= """
    SELECT 
        c.first_name || ' ' || c.last_name AS customer_name,
        COUNT(o.id) AS total_orders,
        SUM(o.total_price) AS total_spent
    FROM 
        orders o
    JOIN 
        customers c ON o.customer_id = c.id
    GROUP BY 
        c.id, customer_name
    ORDER BY 
        total_spent DESC
    LIMIT 10;
    """

cambios_realizados_por_mes_query= """
    SELECT 
        DATE_TRUNC('month', change_date) AS month,
        COUNT(*) AS total_changes
    FROM 
        order_changes
    GROUP BY 
        DATE_TRUNC('month', change_date)
    ORDER BY 
        month;
    """


edad_promedio_por_genero_query="""
    SELECT 
        sex,
        AVG(EXTRACT(YEAR FROM AGE(birth_date))) AS avg_age
    FROM 
        customers
    GROUP BY 
        sex;
    """  
costo_total_por_prenda_query="""
    SELECT 
        g.name AS garment_name,
        SUM((dn->>'quantity')::INTEGER * (dn->>'price')::NUMERIC) AS total_cost
    FROM 
        notes n,
        JSONB_ARRAY_ELEMENTS(detail_notes) dn
    JOIN 
        garments g ON g.id = (dn->>'garmentId')
    GROUP BY 
        g.name
    ORDER BY 
        total_cost DESC;
    """

promedio_prendas_por_pedido_query="""
    SELECT 
        AVG(jsonb_array_length(order_items)) AS avg_items_per_order
    FROM 
        orders;
    """

total_ventas_mes_actual_query="""
    SELECT 
        SUM(total_price) AS total_sales
    FROM 
        orders
    WHERE 
        status = 'entregado' AND
        DATE_TRUNC('month', order_date) = DATE_TRUNC('month', CURRENT_DATE);
    """




pedidos_ultimo_mes_query = """
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        SUM(total_price) AS total_sales
    FROM 
        orders
    WHERE 
        status = 'entregado'
    GROUP BY 
        DATE_TRUNC('month', order_date)
    ORDER BY 
        month;
    """ 

