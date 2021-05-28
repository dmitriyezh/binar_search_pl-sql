declare
 TYPE arr_type IS TABLE OF number
 INDEX BY PLS_INTEGER;
 arr arr_type;
 low number;
 high number;
 mid number;
 guess number;
 item number:=3; -- число которое ищем
begin
-- задаём элементы массива
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
        mid:=floor((low+high)/2); -- округляем без остатка в меньшую сторону
        --DBMS_OUTPUT.put_line('mid='||mid);  
        guess:=arr(mid);
        --DBMS_OUTPUT.put_line('guess='||guess); 
        
        IF guess=item THEN   
        exit; -- ответ в mid           
        END IF;
        
        IF  guess > item THEN -- много, уменьшаем верхнюю границу
        high:= mid - 1;
        --DBMS_OUTPUT.put_line('high='||high); 
        ELSE
        low:=mid+1; -- мало, увеличиваем нижнюю границу
        --DBMS_OUTPUT.put_line('low='||low); 
        END IF;
        
    END LOOP;
    
    DBMS_OUTPUT.put_line('Искомое число находится во '||mid||'й позиции в заданном массиве.'); 
 
end;
