package com.zuikc.SparkMachineLearning;


import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
/*import org.jpmml.model.PMMLUtil;*/

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PMMLDemo2 {
    private Evaluator loadPmml(){
        PMML pmml = new PMML();
        try(InputStream inputStream = new FileInputStream("hdfs://hadoop-01:9000/xhl/data/test5/part-00000.xml")){
            pmml = org.jpmml.model.PMMLUtil.unmarshal(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        return modelEvaluatorFactory.newModelEvaluator(pmml);
    }
    private Object predict(Evaluator evaluator,int a, int b, int c, int d) {
        Map<String, Integer> data = new HashMap<String, Integer>();
        data.put("field_0", a);
        data.put("field_1", b);
        data.put("field_2", c);
        data.put("field_3", d);
        List<InputField> inputFields = evaluator.getInputFields();
            //模型的原始特征，从画像中获取数据，作为模型输入
            Map<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();
            for (InputField inputField : inputFields) {
                FieldName inputFieldName = inputField.getName();
                Object rawValue = data.get(inputFieldName.getValue());
                FieldValue inputFieldValue = inputField.prepare(rawValue);
                arguments.put(inputFieldName, inputFieldValue);
            }

            Map<FieldName, ?> results = evaluator.evaluate(arguments);

            List<TargetField> targetFields = evaluator.getTargetFields();
            TargetField targetField = targetFields.get(0);
            FieldName targetFieldName = targetField.getName();
            ProbabilityDistribution target = (ProbabilityDistribution) results.get(targetFieldName);
            System.out.println(a + " " + b + " " + c + " " + d + ":" + target);
            return target;
    }
    public static void main(String args[]){
        PMMLDemo2 demo = new PMMLDemo2();
        Evaluator model = demo.loadPmml();
        demo.predict(model,2,5,6,8);
        demo.predict(model,7,9,3,6);
        demo.predict(model,1,2,3,1);
        demo.predict(model,2,4,1,5);
    }
}
