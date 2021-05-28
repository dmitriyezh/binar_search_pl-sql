declare
 TYPE arr_type IS TABLE OF number
 INDEX BY PLS_INTEGER;
 arr arr_type;
 low number;
 high number;
 mid number;
 guess number;
 item number:=3; -- ����� ������� ����
begin
-- ����� �������� �������
    arr(1):= 1;
    arr(2):= 3;
    arr(3):= 5;
    arr(4):= 7;
    arr(5):= 9;
    
    low:=arr.first;
    --DBMS_OUTPUT.put_line('low='||low); 
    high:=arr.last;
    --DBMS_OUTPUT.put_line('high='||high); 
   
    WHILE (low <= high)
        LOOP
        mid:=floor((low+high)/2); -- ��������� ��� ������� � ������� �������
        --DBMS_OUTPUT.put_line('mid='||mid);  
        guess:=arr(mid);
        --DBMS_OUTPUT.put_line('guess='||guess); 
        
        IF guess=item THEN   
        exit; -- ����� � mid           
        END IF;
        
        IF  guess > item THEN -- �����, ��������� ������� �������
        high:= mid - 1;
        --DBMS_OUTPUT.put_line('high='||high); 
        ELSE
        low:=mid+1; -- ����, ����������� ������ �������
        --DBMS_OUTPUT.put_line('low='||low); 
        END IF;
        
    END LOOP;
    
    DBMS_OUTPUT.put_line('������� ����� ��������� �� '||mid||'� ������� � �������� �������.'); 
 
end;
